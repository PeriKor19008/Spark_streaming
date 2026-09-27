// Καθαρισμός των collections πριν από ένα νέο πλήρες πείραμα.
// Πρέπει να συνοδεύεται από διαγραφή του container spark-job, ώστε να χαθούν
// και τα checkpoints του streaming query (βλ. task «6. Καθαρό reset»).

const d = db.getSiblingDB("traffic");
["raw_data", "stats", "window_stats"].forEach((c) => d.getCollection(c).drop());
print("reset OK — collections: " + d.getCollectionNames().length);
