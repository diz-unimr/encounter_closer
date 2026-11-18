use anyhow::{Context, Result};
use encounter_closer::{load_settings, Closer, KafkaSink};
use mongodb::{bson::Document, Client};
use rdkafka::{producer::FutureProducer, ClientConfig};
use tracing::{error, info, warn};



#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = load_settings()?;
    info!(
        "Start: interval={:?}, batch_size={}, levels={}",
        cfg.interval, cfg.batch_size, cfg.levels
    );

    let mongo = Client::with_uri_str(&cfg.mongo_uri).await?;
    let coll = mongo
        .database(&cfg.mongo_db)
        .collection::<Document>(&cfg.collection);

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &cfg.kafka_bootstrap_servers)
        .set("acks", "all")
        .set("enable.idempotence", "true")
       // .set("security.protocol","plaintext")
        .create()
        .context("Kafka-Producer konnte nicht erstellt werden")?;

    let sink = KafkaSink { producer, timeout: cfg.kafka_send_timeout };
    let (initial_delay, interval) = (cfg.initial_delay, cfg.interval);
    let closer = Closer::new(coll, sink, cfg);

    let job = async {
        tokio::time::sleep(initial_delay).await;
        loop {
            if let Err(e) = closer.run_once().await {
                error!("Encounter-Abschluss fehlgeschlagen: {e:#}");
            }
            // Fixed delay: nächster Lauf erst nach Ende + interval, keine Überlappung
            tokio::time::sleep(interval).await;
        }
    };

    tokio::select! {
        _ = job => {}
        _ = tokio::signal::ctrl_c() => warn!("Shutdown-Signal empfangen"),
    }
    Ok(())
}