// Demo-Daten (mongosh). period.end hier als ISO-String; die Anwendung kopiert den
// Wert unverändert, funktioniert also auch mit BSON-Dates.
db = db.getSiblingDB("fhir");
db.Encounter.drop();
db.Encounter.createIndex({ fhirId: 1 }, { unique: true });
db.Encounter.createIndex(
    { "period.end": 1, _id: 1 },
    { partialFilterExpression: { "resource.partOf.reference": { $exists: true } } }
);

db.Encounter.insertMany([
    // Fall 1: Einrichtung beendet, Abteilung + Versorgungsstelle offen -> beide werden geschlossen
    { fhirId: "einr-1", resource: { status: "finished",    class: { code : "IMP" , system: "some system" }, period: { start: "2025-01-01T08:00:00Z", end: "2025-01-10T12:00:00Z" } } },
    { fhirId: "abt-1",  resource: { status: "in-progress", class: { code : "IMP" , system: "some system" }, period: { start: "2025-01-01T08:00:00Z" }, partOf: { reference: "Encounter/einr-1" } } },
    { fhirId: "vers-1", resource: { status: "in-progress", class: { code : "IMP" , system: "some system" }, period: { start: "2025-01-02T09:00:00Z" }, partOf: { reference: "Encounter/abt-1" } } },

    // Fall 2: Einrichtung noch offen -> nichts passiert
    { fhirId: "einr-2", resource: { status: "in-progress", class: { code : "IMP" , system: "some system" }, period: { start: "2025-02-01T08:00:00Z" } } },
    { fhirId: "abt-2",  resource: { status: "in-progress", class: { code : "IMP" , system: "some system" }, period: { start: "2025-02-01T08:00:00Z" }, partOf: { reference: "Encounter/einr-2" } } },

    // Fall 3: Kind war bereits selbst beendet -> behält sein Ende
    { fhirId: "einr-3", resource: { status: "finished", class: { code : "IMP" , system: "some system" }, period: { start: "2025-03-01T08:00:00Z", end: "2025-03-10T12:00:00Z" } } },
    { fhirId: "abt-3",  resource: { status: "finished", class: { code : "IMP" , system: "some system" }, period: { start: "2025-03-01T08:00:00Z", end: "2025-03-05T10:00:00Z" }, partOf: { reference: "Encounter/einr-3" } } },

    // Fall 1: Einrichtung beendet, Abteilung + Versorgungsstelle offen aber class is AMB -> bleibt offen AMB wird ignoriert
    { fhirId: "einr-4", resource: { status: "finished",  class: { code : "AMB" , system: "some system" },  period: { start: "2025-01-01T08:00:00Z", end: "2025-01-10T12:00:00Z" } } },
    { fhirId: "abt-4",  resource: { status: "in-progress", class: { code : "AMB" , system: "some system" }, period: { start: "2025-01-01T08:00:00Z" }, partOf: { reference: "Encounter/einr-4" } } },
    { fhirId: "vers-4", resource: { status: "in-progress", class: { code : "AMB" , system: "some system" }, period: { start: "2025-01-02T09:00:00Z" }, partOf: { reference: "Encounter/abt-4" } } },
]);

print("Seed fertig: " + db.Encounter.countDocuments() + " Encounter");