use std::{
    collections::{HashMap, HashSet},
    future::Future,
    time::Duration,
};

use anyhow::{Context, Result};
use futures::{TryStreamExt, future::join_all};
use mongodb::{
    Collection,
    bson::{Bson, Document, doc},
};
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
};
use serde::Deserialize;
use tracing::{debug, error, info};

// ---- Feldnamen (an einer Stelle änderbar) ----
pub const F_ID: &str = "_id";
pub const F_FHIR_ID: &str = "fhirId";
pub const F_STATUS: &str = "resource.status";
pub const F_BED_STATUS: &str = "resource.class.code";
pub const F_PERIOD_END: &str = "resource.period.end";
pub const F_PART_OF_REF: &str = "resource.partOf.reference";
pub const F_KAFKA_PENDING: &str = "kafkaPending"; // Outbox-Flag

// ---- Konfiguration ----
#[derive(Debug, Clone, Deserialize)]
pub struct Settings {
    #[serde(with = "humantime_serde", default = "default_interval")]
    pub interval: Duration,
    #[serde(with = "humantime_serde", default = "default_initial_delay")]
    pub initial_delay: Duration,
    #[serde(default = "default_batch_size")]
    pub batch_size: i64,
    #[serde(default = "default_levels")]
    pub levels: u32,
    #[serde(default = "default_collection")]
    pub collection: String,
    pub target_topic: String,
    #[serde(with = "humantime_serde", default = "default_send_timeout")]
    pub kafka_send_timeout: Duration,
    pub mongo_uri: String,
    pub mongo_db: String,
    pub kafka_bootstrap_servers: String,
}

fn default_interval() -> Duration {
    Duration::from_secs(24 * 3600)
}
fn default_initial_delay() -> Duration {
    Duration::from_secs(60)
}
fn default_batch_size() -> i64 {
    200
}
fn default_levels() -> u32 {
    2
}
fn default_collection() -> String {
    "Encounter".into()
}
fn default_send_timeout() -> Duration {
    Duration::from_secs(30)
}

pub fn load_settings() -> Result<Settings> {
    config::Config::builder()
        .add_source(config::File::with_name("config").required(false))
        .add_source(config::Environment::with_prefix("ENCOUNTER_CLOSER").separator("__"))
        .build()?
        .try_deserialize::<Settings>()
        .context("Konfiguration ungültig")
}

// ---- Kafka-Abstraktion (in Tests ersetzbar) ----
pub trait EventSink: Send + Sync {
    fn send(
        &self,
        topic: &str,
        key: &str,
        payload: &str,
    ) -> impl Future<Output = Result<()>> + Send;
}

pub struct KafkaSink {
    pub producer: FutureProducer,
    pub timeout: Duration,
}

impl EventSink for KafkaSink {
    async fn send(&self, topic: &str, key: &str, payload: &str) -> Result<()> {
        let record = FutureRecord::to(topic).key(key).payload(payload);
        self.producer
            .send(record, Timeout::After(self.timeout))
            .await
            .map(|_| ())
            .map_err(|(e, _)| anyhow::anyhow!(e))
    }
}

// ---- Service ----
pub struct Closer<S: EventSink> {
    coll: Collection<Document>,
    sink: S,
    cfg: Settings,
}

impl<S: EventSink> Closer<S> {
    pub fn new(coll: Collection<Document>, sink: S, cfg: Settings) -> Self {
        Self { coll, sink, cfg }
    }

    /// Ein kompletter Lauf: erst offene Kafka-Nachzügler, dann `levels` Durchläufe.
    pub async fn run_once(&self) -> Result<usize> {
        let resent = self.resend_pending().await?;
        if resent > 0 {
            info!("{resent} ausstehende Events nachproduziert");
        }

        let mut total = 0;
        for level in 1..=self.cfg.levels {
            let n = self.close_open_children_of_ended_parents().await?;
            info!("Stufe {level}: {n} Encounter abgeschlossen");
            total += n;
        }
        info!("Lauf beendet, insgesamt {total} Encounter abgeschlossen");
        Ok(total)
    }

    async fn close_open_children_of_ended_parents(&self) -> Result<usize> {
        let mut closed = 0usize;
        let mut last_id: Option<Bson> = None;

        loop {
            // 1) Batch offener Kinder (Keyset-Paginierung über _id).
            //    `period.end: null` matcht fehlende und null-Felder.
            let mut filter = doc! {
                F_PERIOD_END: Bson::Null,
                F_PART_OF_REF: { "$exists": true, "$ne": Bson::Null },
            };
            if let Some(id) = &last_id {
                filter.insert(F_ID, doc! { "$gt": id.clone() });
            }

            let children: Vec<Document> = self
                .coll
                .find(filter)
                .sort(doc! { F_ID: 1 })
                .limit(self.cfg.batch_size)
                .await?
                .try_collect()
                .await?;
            if children.is_empty() {
                break;
            }
            last_id = children.last().and_then(|d| d.get(F_ID).cloned());

            // 2) Eltern in einer Abfrage laden, nur abgeschlossene
            let parent_ids: HashSet<String> = children.iter().filter_map(parent_fhir_id).collect();

            let mut parent_end: HashMap<String, Bson> = HashMap::new();
            if !parent_ids.is_empty() {
                let ids: Vec<String> = parent_ids.into_iter().collect();
                let parents: Vec<Document> = self
                    .coll
                    .find(doc! {
                        F_FHIR_ID: { "$in": ids },
                        F_BED_STATUS: { "$in": Bson::Array(vec![Bson::String("IMP".into()), Bson::String("NS".into()),Bson::String("SS".into())])},
                        F_PERIOD_END: { "$ne": Bson::Null },
                    })
                    .projection(doc! { F_FHIR_ID: 1, F_PERIOD_END: 1 })
                    .await?
                    .try_collect()
                    .await?;
                for p in parents {
                    if let (Ok(fid), Some(end)) = (p.get_str(F_FHIR_ID), get_end(&p)) {
                        parent_end.insert(fid.to_string(), end);
                    }
                }
            }

            // 3) Kinder abschließen (optimistisch: nur wenn noch offen)
            let mut updated: Vec<Document> = Vec::new();
            for mut child in children {
                let Some(end) = parent_fhir_id(&child).and_then(|p| parent_end.get(&p).cloned())
                else {
                    continue; // Elternteil offen oder nicht gefunden
                };

                let res = self
                    .coll
                    .update_one(
                        doc! { F_ID: child.get(F_ID).cloned().unwrap_or(Bson::Null),
                        F_PERIOD_END: Bson::Null },
                        doc! { "$set": {
                            F_PERIOD_END: end.clone(),
                            F_STATUS: "finished",
                            F_KAFKA_PENDING: true,
                        }},
                    )
                    .await?;

                if res.modified_count == 1 {
                    set_end(&mut child, end);
                    child.insert(F_STATUS, "finished");
                    updated.push(child);
                }
            }

            // 4) Nach erfolgreichem Mongo-Update nach Kafka produzieren
            closed += updated.len();
            let sent = self.send_and_clear_flag(updated).await?;
            debug!("Batch: {sent} Events bestätigt");
        }
        Ok(closed)
    }

    /// Holt Dokumente nach, deren Kafka-Send in einem früheren Lauf scheiterte.
    async fn resend_pending(&self) -> Result<usize> {
        let mut total = 0usize;
        let mut last_id: Option<Bson> = None;

        loop {
            let mut filter = doc! { F_KAFKA_PENDING: true };
            if let Some(id) = &last_id {
                filter.insert(F_ID, doc! { "$gt": id.clone() });
            }
            let docs: Vec<Document> = self
                .coll
                .find(filter)
                .sort(doc! { F_ID: 1 })
                .limit(self.cfg.batch_size)
                .await?
                .try_collect()
                .await?;
            if docs.is_empty() {
                break;
            }
            last_id = docs.last().and_then(|d| d.get(F_ID).cloned());
            total += self.send_and_clear_flag(docs).await?;
        }
        Ok(total)
    }

    /// Sendet parallel; entfernt das Outbox-Flag nur für bestätigte Sends.
    async fn send_and_clear_flag(&self, docs: Vec<Document>) -> Result<usize> {
        if docs.is_empty() {
            return Ok(0);
        }
        let results = join_all(docs.into_iter().map(|d| self.send(d))).await;

        let ok_ids: Vec<Bson> = results.into_iter().flatten().collect();
        if !ok_ids.is_empty() {
            self.coll
                .update_many(
                    doc! { F_ID: { "$in": ok_ids.clone() } },
                    doc! { "$unset": { F_KAFKA_PENDING: "" } },
                )
                .await?;
        }
        Ok(ok_ids.len())
    }

    /// Gibt bei Erfolg die `_id` zurück, bei Fehler `None` (Flag bleibt gesetzt).
    async fn send(&self, d: Document) -> Option<Bson> {
        let id = d.get(F_ID).cloned()?;
        let (key, payload) = match build_payload(d) {
            Ok(v) => v,
            Err(e) => {
                error!("Serialisierung fehlgeschlagen: {e:#}");
                return None;
            }
        };
        match self.sink.send(&self.cfg.target_topic, &key, &payload).await {
            Ok(()) => Some(id),
            Err(e) => {
                error!("Kafka-Send fehlgeschlagen für {key}: {e:#}");
                None
            }
        }
    }
}

// ---- Hilfsfunktionen ----

/// `Encounter/abc` -> `abc`, reine ID bleibt unverändert.
pub fn parent_fhir_id(d: &Document) -> Option<String> {
    let reference = d.get_document("partOf").ok()?.get_str("reference").ok()?;
    reference.rsplit('/').next().map(str::to_string)
}

pub fn get_end(d: &Document) -> Option<Bson> {
    d.get_document("period")
        .ok()?
        .get("end")
        .filter(|b| !matches!(b, Bson::Null))
        .cloned()
}

pub fn set_end(d: &mut Document, end: Bson) {
    match d.get_document_mut("period") {
        Ok(p) => {
            p.insert("end", end);
        }
        Err(_) => {
            d.insert("period", doc! { "end": end });
        }
    }
}

/// Liefert (Kafka-Key, JSON-Payload); das Outbox-Flag wird nicht mitgesendet.
pub fn build_payload(mut d: Document) -> Result<(String, String)> {
    d.remove(F_KAFKA_PENDING);
    let key = d.get_str(F_FHIR_ID).unwrap_or_default().to_string();
    let json = serde_json::to_string(&Bson::Document(d).into_relaxed_extjson())?;
    Ok((key, json))
}

// ---- Unit-Tests (kein Docker nötig) ----
#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::DateTime;

    #[test]
    fn parent_id_aus_referenz_mit_praefix() {
        let d = doc! { "partOf": { "reference": "Encounter/abc-123" } };
        assert_eq!(parent_fhir_id(&d).as_deref(), Some("abc-123"));
    }

    #[test]
    fn parent_id_aus_reiner_id() {
        let d = doc! { "partOf": { "reference": "abc-123" } };
        assert_eq!(parent_fhir_id(&d).as_deref(), Some("abc-123"));
    }

    #[test]
    fn parent_id_fehlt_ohne_partof() {
        assert_eq!(parent_fhir_id(&doc! { "fhirId": "x" }), None);
        assert_eq!(parent_fhir_id(&doc! { "partOf": {} }), None);
        assert_eq!(parent_fhir_id(&doc! { "partOf": { "reference": 5 } }), None);
    }

    #[test]
    fn get_end_liefert_wert_oder_none() {
        let dt = DateTime::from_millis(1_700_000_000_000);
        assert_eq!(
            get_end(&doc! { "period": { "end": dt } }),
            Some(Bson::DateTime(dt))
        );
        assert_eq!(get_end(&doc! { "period": { "end": Bson::Null } }), None);
        assert_eq!(get_end(&doc! { "period": { "start": dt } }), None);
        assert_eq!(get_end(&doc! {}), None);
    }

    #[test]
    fn get_end_unterstuetzt_strings() {
        let d = doc! { "period": { "end": "2025-01-31T10:00:00Z" } };
        assert_eq!(
            get_end(&d),
            Some(Bson::String("2025-01-31T10:00:00Z".into()))
        );
    }

    #[test]
    fn set_end_ergaenzt_bestehendes_period() {
        let mut d = doc! { "period": { "start": "2025-01-01" } };
        set_end(&mut d, Bson::String("2025-01-31".into()));
        let p = d.get_document("period").unwrap();
        assert_eq!(p.get_str("start").unwrap(), "2025-01-01"); // bleibt erhalten
        assert_eq!(p.get_str("end").unwrap(), "2025-01-31");
    }

    #[test]
    fn set_end_legt_period_an_wenn_es_fehlt() {
        let mut d = doc! { "fhirId": "x" };
        set_end(&mut d, Bson::String("2025-01-31".into()));
        assert_eq!(
            d.get_document("period").unwrap().get_str("end").unwrap(),
            "2025-01-31"
        );
    }

    #[test]
    fn payload_enthaelt_kein_outbox_flag_und_key_ist_fhir_id() {
        let d = doc! { "fhirId": "enc-1", "status": "finished", F_KAFKA_PENDING: true };
        let (key, json) = build_payload(d).unwrap();
        assert_eq!(key, "enc-1");
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["status"], "finished");
        assert!(v.get(F_KAFKA_PENDING).is_none());
    }
}
