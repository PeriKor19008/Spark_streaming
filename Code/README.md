# Επεξεργασία Δεδομένων Ροής με Kafka, Spark και MongoDB

Υλοποίηση του project του εαρινού εξαμήνου 2026 για το μάθημα
*Συστήματα Διαχείρισης Μεγάλων Δεδομένων (CEID_NE4348)*.

```
UXSIM ──► Redpanda ──► Spark Structured Streaming ──► MongoDB
       (producer.py)   (spark_streaming.py)       (traffic.raw_data / .stats)
```

## Αρχεία

| Αρχείο | Περιεχόμενο |
|---|---|
| `producer.py` | Ερώτημα 1 — εξομοίωση UXSIM και αποστολή σε Redpanda |
| `spark_streaming.py` | Ερωτήματα 2 & 3 — Spark consumer, στατιστικά, MongoDB sinks |
| `api.py` | Bonus — REST API πάνω από τη MongoDB |
| `docker-compose.yml` | Redpanda, Spark master/worker, spark-job, MongoDB |
| `requirements.txt` | Εξαρτήσεις για τον host |
| `run_uxsim.py`, `spark_consumer_PC.py` | Τα αρχικά αρχεία που δόθηκαν (αναφορά) |

## Εκτέλεση

**1. Εξαρτήσεις στον host**

```bash
pip install -r requirements.txt
```

**2. Εκκίνηση των υποδομών**

```bash
docker compose up -d
```

**3. Δημιουργία του topic**

```bash
docker exec redpanda rpk topic create vehicle_positions
```

Το `spark-job` έχει `restart: on-failure`, οπότε αν ξεκίνησε πριν υπάρξει το
topic θα επανασυνδεθεί μόνο του μόλις αυτό δημιουργηθεί.

**4. Εκκίνηση του producer**

```bash
python producer.py --tmax 3600 --interval 5 --speedup 10
```

| Παράμετρος | Προεπιλογή | Περιγραφή |
|---|---|---|
| `--bootstrap` | `localhost:19092` | διεύθυνση του broker |
| `--topic` | `vehicle_positions` | όνομα topic |
| `--interval` | `5` | **Ν**: δευτερόλεπτα εξομοίωσης ανά κύκλο αποστολής |
| `--speedup` | `10` | επιτάχυνση ως προς τον πραγματικό χρόνο |
| `--tmax` | `3600` | συνολική διάρκεια εξομοίωσης |
| `--seed` | `42` | random seed |
| `--include-stopped` | off | αποστολή και των σταματημένων οχημάτων |

**5. Παρακολούθηση**

```bash
docker logs -f spark-job
```

```bash
docker exec redpanda rpk topic consume vehicle_positions --num 5
```

**6. Έλεγχος της MongoDB**

```bash
docker cp queries.js mongo:/tmp/queries.js; docker exec mongo mongosh --quiet /tmp/queries.js
```

> Σε PowerShell απόφυγε το `mongosh --quiet < queries.js`: το `<` δεν υποστηρίζεται
> ως τελεστής ανακατεύθυνσης, και το inline `--eval "..."` χάνει τα εσωτερικά
> εισαγωγικά. Η αντιγραφή του αρχείου στο container δουλεύει σε κάθε shell.

> Προσοχή: στη `mongosh` το `db.stats` είναι built-in μέθοδος, οπότε η collection
> `stats` προσπελαύνεται μόνο ως `db.getCollection('stats')`.

**7. Bonus — REST API**

```bash
uvicorn api:app --port 8000
```

Στη συνέχεια `http://localhost:8000/links` ή `http://localhost:8000/docs`.

## Δομή των collections

**`traffic.raw_data`** — ένα document ανά όχημα ανά στιγμιότυπο, όπως στάλθηκε:

```json
{"name": "4", "dn": 5, "orig": "E1", "dest": "W1", "t": 15,
 "link": "E1I4", "x": 250, "s": -1, "v": 50}
```

**`traffic.stats`** — ένα document ανά (χρονική στιγμή, ακμή):

```json
{"time": 85, "link": "I3I2", "vcount": 6, "vspeed": 32.5, "vmax": 50, "vmin": 15}
```

**`traffic.window_stats`** — bonus, παράθυρα 30 δευτερολέπτων:

```json
{"window_start": 0, "window_end": 30, "link": "S4I4", "vcount": 1, "vspeed": 30}
```

> Ένα παράθυρο 30s καλύπτει έξι χρονικά βήματα, που μπορεί να φτάσουν σε
> διαφορετικά micro-batches. Με `outputMode("update")` το group ξαναγράφεται
> τότε με ενημερωμένες τιμές, οπότε η collection κρατά διαδοχικές εκδόσεις του
> ίδιου κλειδιού — κατά την ανάγνωση ισχύει η **τελευταία**. Η `stats` δεν
> εμφανίζει το φαινόμενο, επειδή όλα τα οχήματα μιας χρονικής στιγμής
> αποστέλλονται μαζί.

## Καθαρό τρέξιμο από την αρχή

Το streaming aggregation κρατά state στα checkpoints. Αν ξανατρέξεις τον
producer χωρίς να επανεκκινήσεις το Spark job, τα groups της προηγούμενης
εκτέλεσης **αθροίζονται** με τα νέα. Για καθαρή μέτρηση:

```bash
docker compose rm -sf spark-submit-job; docker cp reset.js mongo:/tmp/reset.js; docker exec mongo mongosh --quiet /tmp/reset.js; docker compose up -d spark-submit-job
```

Δώσε ~30 δευτερόλεπτα στο `spark-job` να συνδεθεί στον broker πριν ξεκινήσεις
τον producer, αλλιώς τα πρώτα records θα σταλούν πριν αρχίσει να ακούει.

## Καθαρισμός

```bash
docker compose down --remove-orphans
```
