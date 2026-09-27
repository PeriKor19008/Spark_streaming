"""
Ερωτήματα 2 & 3 — Κατανάλωση από Redpanda με Spark Structured Streaming,
υπολογισμός στατιστικών ανά ακμή και αποθήκευση σε MongoDB.

Ροή:
    Redpanda (topic: vehicle_positions)
        -> readStream (Kafka source)
        -> CAST(value AS STRING) -> from_json(schema)
        -> raw_data   (MongoDB, όπως έρχονται)
        -> groupBy(time, link) -> vcount / vspeed
        -> stats      (MongoDB)
        -> windowed aggregation 30s (bonus) -> window_stats (MongoDB)

Εκτελείται μέσα στο container spark-job μέσω spark-submit (βλ. docker-compose.yml).
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, from_json
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import window
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType

# ---------------------------------------------------------------------------
# Ρυθμίσεις (παραμετροποιήσιμες μέσω environment variables)
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
TOPIC = os.getenv("TOPIC", "vehicle_positions")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.getenv("MONGO_DB", "traffic")
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_ROOT", "/tmp/checkpoints")
WINDOW_SIZE = os.getenv("WINDOW_SIZE", "30 seconds")
CONSOLE_SINK = os.getenv("CONSOLE_SINK", "1") == "1"

# 1. Ορισμός του Schema
## vehicles_to_pandas returns a pd.DataFrame:
#             A DataFrame containing the travel logs of vehicles, with the columns:
#
#             - 'name': the name of the vehicle (platoon).
#             - 'dn': the platoon size.
#             - 'orig': the origin node of the vehicle's trip.
#             - 'dest': the destination node of the vehicle's trip.
#             - 't': the timestep.
#             - 'link': the link the vehicle is on (or relevant status).
#             - 'x': the position of the vehicle on the link.
#             - 's': the spacing of the vehicle.
#             - 'v': the speed of the vehicle.
schema = (
    StructType()
    .add("name", StringType())
    .add("dn", IntegerType())
    .add("orig", StringType())
    .add("dest", StringType())
    .add("t", DoubleType())
    .add("link", StringType())
    .add("x", DoubleType())
    .add("s", DoubleType())
    .add("v", DoubleType())
)

# ---------------------------------------------------------------------------
# 2. Δημιουργία Spark Session
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("UXSIM-Consumer")
    .config("spark.mongodb.write.connection.uri", MONGO_URI)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 3. Σύνδεση στον Redpanda (Kafka-compatible)
# ---------------------------------------------------------------------------
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# ---------------------------------------------------------------------------
# 4. Parsing του JSON και Μετασχηματισμός
# ---------------------------------------------------------------------------
# Ο Kafka source επιστρέφει το payload ως binary στη στήλη `value`. Το κάνουμε
# cast σε string, εφαρμόζουμε το schema (δημιουργώντας ένα struct `data`) και
# το "ξεδιπλώνουμε" ώστε κάθε key του JSON να γίνει ξεχωριστή στήλη.
parsed = (
    df.selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
    .filter(col("link").isNotNull())
)

# Μετονομασία στα ονόματα που ζητά η εκφώνηση: t -> time, v -> speed.
# Το `event_time` είναι το simulation time εκφρασμένο ως TimestampType, ώστε να
# μπορεί να χρησιμοποιηθεί από τη συνάρτηση window() του Spark (bonus).
data = (
    parsed
    .withColumnRenamed("t", "time")
    .withColumnRenamed("v", "speed")
    .withColumn("event_time", col("time").cast("timestamp"))
)

# ---------------------------------------------------------------------------
# 5. Υπολογισμός Στατιστικών ανά Ακμή (link) και Χρόνο (time)
# ---------------------------------------------------------------------------
stats = (
    data.groupBy("time", "link")
    .agg(
        count("*").alias("vcount"),
        avg("speed").alias("vspeed"),
        spark_max("speed").alias("vmax"),   # bonus: μέγιστη ταχύτητα ανά link
        spark_min("speed").alias("vmin"),   # bonus: ελάχιστη ταχύτητα ανά link
    )
)

# Bonus: windowed aggregation — μέσες ταχύτητες ανά ακμή σε παράθυρα 30s
# πάνω στον χρόνο της εξομοίωσης.
windowed_stats = (
    data.groupBy(window(col("event_time"), WINDOW_SIZE), col("link"))
    .agg(
        count("*").alias("vcount"),
        avg("speed").alias("vspeed"),
    )
    .select(
        col("window.start").cast("double").alias("window_start"),
        col("window.end").cast("double").alias("window_end"),
        col("link"),
        col("vcount"),
        col("vspeed"),
    )
)


# ---------------------------------------------------------------------------
# 6. MongoDB sinks
# ---------------------------------------------------------------------------
def mongo_writer(collection, preview=False):
    """Επιστρέφει συνάρτηση foreachBatch που γράφει το micro-batch σε collection."""

    def _write(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        (
            batch_df.write.format("mongodb")
            .mode("append")
            .option("database", MONGO_DB)
            .option("collection", collection)
            .save()
        )
        print(f"[{collection}] Batch {batch_id} processed: {batch_df.count()} rows")
        if preview:
            batch_df.show(5, truncate=False)

    return _write


def start(stream_df, collection, output_mode, preview=False):
    return (
        stream_df.writeStream
        .foreachBatch(mongo_writer(collection, preview))
        .outputMode(output_mode)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/{collection}")
        .start()
    )


# 6α. Τα raw δεδομένα όπως έρχονται από τον Redpanda.
query_raw = start(parsed, "raw_data", "append")

# 6β. Τα επεξεργασμένα δεδομένα.
# Το outputMode("append") σε streaming aggregation απαιτεί watermark για να ξέρει
# το Spark πότε "έκλεισε" ένα group. Επειδή το `time` είναι χρόνος εξομοίωσης και
# όχι πραγματικός χρόνος ρολογιού, χρησιμοποιούμε outputMode("update"), που γράφει
# μόνο τα groups των οποίων οι τιμές άλλαξαν στο τρέχον micro-batch.
query_stats = start(stats, "stats", "update", preview=True)
query_windowed = start(windowed_stats, "window_stats", "update")

# ---------------------------------------------------------------------------
# 7. Προβολή στην κονσόλα για debugging (προαιρετικά)
# ---------------------------------------------------------------------------
if CONSOLE_SINK:
    query_console = (
        stats.writeStream
        .format("console")
        .outputMode("update")
        .option("truncate", "false")
        .option("numRows", 20)
        .start()
    )

# Αναμονή για τον τερματισμό όλων των queries
spark.streams.awaitAnyTermination()
