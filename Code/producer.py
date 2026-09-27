"""
Ερώτημα 1 — Παραγωγή δεδομένων κίνησης με τον εξομοιωτή UXSIM και αποστολή
τους σε Redpanda (Kafka-compatible broker).

Σε αντίθεση με μια προσέγγιση "τρέξε όλη την εξομοίωση και μετά κάνε replay",
εδώ ο UXSIM καλείται *επαναληπτικά*: κάθε Ν δευτερόλεπτα η εξομοίωση προχωράει
κατά Ν δευτερόλεπτα και στέλνεται ένα στιγμιότυπο (snapshot) μόνο των οχημάτων
που βρίσκονται εκείνη τη στιγμή στο δίκτυο. Έτσι τα δεδομένα παράγονται
πραγματικά σε ροή, όπως ζητά η εκφώνηση (§3.4).

Χρήση:
    python producer.py                          # προεπιλογές
    python producer.py --interval 10 --speedup 20
    python producer.py --bootstrap localhost:19092 --topic vehicle_positions
"""

import argparse
import json
import logging
import random
import sys
import time

import numpy as np
from kafka import KafkaProducer
from kafka.errors import KafkaError
from uxsim import World

# Η kafka-python 3.x αφαίρεσε το NoBrokersAvailable και σηματοδοτεί την αποτυχία
# σύνδεσης με KafkaConnectionError / MetadataEmptyBrokerList. Και τα δύο είναι
# υποκλάσεις του KafkaError, οπότε ο κώδικας παρακάτω πιάνει την περίπτωση και
# στις δύο εκδόσεις της βιβλιοθήκης.

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("producer")

# Η kafka-python καταγράφει σε επίπεδο INFO κάθε σύνδεση, ανανέωση metadata και
# κλείσιμο socket. Χρήσιμο μόνο κατά την αποσφαλμάτωση της σύνδεσης, οπότε εδώ
# περιορίζεται σε WARNING ώστε να μένουν ορατά τα μηνύματα της ίδιας της ροής.
logging.getLogger("kafka").setLevel(logging.WARNING)


def convert(obj):
    """Μετατροπή numpy τύπων σε native Python ώστε να είναι JSON-serializable."""
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    return obj


# ---------------------------------------------------------------------------
# Ορισμός σεναρίου εξομοίωσης
# ---------------------------------------------------------------------------
def build_world(tmax, seed):
    ## Build network, vehicles, signals, etc.
    ## Definition of a 4×4 grid‑like traffic network with:
    # Boundary nodes
    #   - North nodes: N1, N2, N3, N4
    #   - South nodes: S1, S2, S3, S4
    #   - West entry : W1
    #   - East exit : E1
    # Internal intersections: I1, I2, I3, I4
    # dn stands for "Distance to the next Node.
    """
        N1  N2  N3  N4
        |   |   |   |
    W1--I1--I2--I3--I4-<E1
        |   |   |   |
        v   ^   v   ^
        S1  S2  S3  S4
    """

    W = World(
        name="",
        deltan=5,
        tmax=tmax,
        print_mode=0,
        save_mode=0,
        show_mode=0,
        random_seed=seed,
        duo_update_time=600,
    )
    random.seed(seed)

    ## The signal parameter defines the traffic light timing cycle for the specific intersection.
    signal_time = 20  # base cycle (seconds)
    sf_1 = 1  # Split Factors. They determine how the total green time is divided between the different directions.
    sf_2 = 2

    I1 = W.addNode("I1", 1, 0, signal=[signal_time * sf_1, signal_time * sf_2])
    I2 = W.addNode("I2", 2, 0, signal=[signal_time * sf_1, signal_time * sf_2])
    I3 = W.addNode("I3", 3, 0, signal=[signal_time * sf_1, signal_time * sf_2])
    I4 = W.addNode("I4", 4, 0, signal=[signal_time * sf_1, signal_time * sf_2])
    W1 = W.addNode("W1", 0, 0)
    E1 = W.addNode("E1", 5, 0)
    N1 = W.addNode("N1", 1, 1)
    N2 = W.addNode("N2", 2, 1)
    N3 = W.addNode("N3", 3, 1)
    N4 = W.addNode("N4", 4, 1)
    S1 = W.addNode("S1", 1, -1)
    S2 = W.addNode("S2", 2, -1)
    S3 = W.addNode("S3", 3, -1)
    S4 = W.addNode("S4", 4, -1)

    # E <-> W direction: signal group 0
    for n1, n2 in [[W1, I1], [I1, I2], [I2, I3], [I3, I4], [I4, E1]]:
        W.addLink(n2.name + n1.name, n2, n1, length=500, free_flow_speed=50,
                  jam_density=0.2, number_of_lanes=3, signal_group=0)

    # N -> S direction: signal group 1
    for n1, n2 in [[N1, I1], [I1, S1], [N3, I3], [I3, S3]]:
        W.addLink(n1.name + n2.name, n1, n2, length=500, free_flow_speed=30,
                  jam_density=0.2, signal_group=1)

    # S -> N direction: signal group 2
    for n1, n2 in [[N2, I2], [I2, S2], [N4, I4], [I4, S4]]:
        W.addLink(n2.name + n1.name, n2, n1, length=500, free_flow_speed=30,
                  jam_density=0.2, signal_group=1)

    # random demand definition every 30 seconds
    dt = 30
    demand = 2
    for t in range(0, tmax, dt):
        dem = random.uniform(0, demand)
        for n1, n2 in [[N1, S1], [S2, N2], [N3, S3], [S4, N4]]:
            W.adddemand(n1, n2, t, t + dt, dem * 0.25)
        for n1, n2 in [[E1, W1], [N1, W1], [S2, W1], [N3, W1], [S4, W1]]:
            W.adddemand(n1, n2, t, t + dt, dem * 0.75)

    return W


# ---------------------------------------------------------------------------
# Στιγμιότυπο της τρέχουσας κατάστασης του δικτύου
# ---------------------------------------------------------------------------
def snapshot(W, only_moving=True):
    """
    Επιστρέφει λίστα από records για τα οχήματα που βρίσκονται αυτή τη στιγμή
    στο δίκτυο. Τα ονόματα των πεδίων είναι ίδια με αυτά που παράγει η
    vehicles_to_pandas() του UXsim, ώστε το schema στον Spark consumer να είναι
    το ίδιο ανεξάρτητα από τον τρόπο παραγωγής.
    """
    t = W.T * W.DELTAT
    records = []

    for veh in W.VEHICLES.values():
        # state: "home" (δεν ξεκίνησε), "wait", "run" (στο δίκτυο), "end"
        if veh.state != "run" or veh.link is None:
            continue

        # Φιλτράρισμα οχημάτων σε κίνηση, όπως ζητά η εκφώνηση (speed > 0)
        if only_moving and veh.v <= 0:
            continue

        records.append({
            "name": str(veh.name),
            "dn": int(W.DELTAN),               # μέγεθος platoon
            "orig": veh.orig.name,
            "dest": veh.dest.name,
            "t": float(t),
            "link": veh.link.name,
            "x": float(veh.x),
            "s": float(veh.log_s[-1]) if veh.log_s else None,
            "v": float(veh.v),
        })

    return t, records


# ---------------------------------------------------------------------------
# Σύνδεση στον broker με επαναλήψεις
# ---------------------------------------------------------------------------
def connect(bootstrap, retries=5, backoff=3):
    """Δημιουργεί KafkaProducer, με επαναπροσπάθειες σε αποτυχία σύνδεσης."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=3,
                acks=1,
            )
            log.info("Συνδέθηκε στον broker %s", bootstrap)
            return producer
        except KafkaError as exc:
            log.warning("Ο broker %s δεν είναι διαθέσιμος: %s (προσπάθεια %d/%d). "
                        "Νέα προσπάθεια σε %ds...",
                        bootstrap, type(exc).__name__, attempt, retries, backoff)
            time.sleep(backoff)

    log.error("Αδύνατη η σύνδεση στον broker %s μετά από %d προσπάθειες. "
              "Τρέχει ο Redpanda; (docker compose up -d)", bootstrap, retries)
    return None


# ---------------------------------------------------------------------------
# Κύριος βρόχος παραγωγής
# ---------------------------------------------------------------------------
def run(args):
    log.info("Κατασκευή σεναρίου εξομοίωσης (tmax=%ds, seed=%s)", args.tmax, args.seed)
    W = build_world(args.tmax, args.seed)

    producer = connect(args.bootstrap, args.retries, args.backoff)
    if producer is None:
        return 1

    sleep_for = args.interval / args.speedup
    log.info("Έναρξη ροής -> topic '%s' | βήμα %ds εξομοίωσης ανά %.2fs πραγματικού χρόνου",
             args.topic, args.interval, sleep_for)

    sent_total = 0
    failed_total = 0
    sim_time = 0

    try:
        while True:
            # Κάθε Ν δευτερόλεπτα καλούμε τον UXSIM για νέα δεδομένα:
            # η εξομοίωση προχωράει κατά Ν δευτερόλεπτα.
            sim_time += args.interval
            finished = W.exec_simulation(until_t=sim_time)

            t, records = snapshot(W, only_moving=not args.include_stopped)

            batch_ok = 0
            for record in records:
                try:
                    producer.send(args.topic, {k: convert(v) for k, v in record.items()})
                    batch_ok += 1
                except KafkaError as exc:
                    failed_total += 1
                    log.error("Αποτυχία αποστολής record: %s", exc)

            try:
                producer.flush(timeout=10)
            except KafkaError as exc:
                log.error("Αποτυχία flush: %s", exc)

            sent_total += batch_ok
            log.info("t=%6.0fs | οχήματα σε κίνηση: %3d | στάλθηκαν: %3d | σύνολο: %d",
                     t, len(records), batch_ok, sent_total)

            if finished:
                log.info("Η εξομοίωση ολοκληρώθηκε στα %ds.", t)
                break

            time.sleep(sleep_for)

    except KeyboardInterrupt:
        log.warning("Διακοπή από τον χρήστη.")
    finally:
        try:
            producer.flush(timeout=10)
            producer.close(timeout=10)
        except KafkaError as exc:
            log.error("Σφάλμα κατά το κλείσιμο του producer: %s", exc)

    log.info("Ολοκληρώθηκε. Records: %d επιτυχή, %d αποτυχημένα.", sent_total, failed_total)
    return 0


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="UXSIM -> Redpanda producer")
    p.add_argument("--bootstrap", default="localhost:19092",
                   help="διεύθυνση του Kafka/Redpanda broker (default: localhost:19092)")
    p.add_argument("--topic", default="vehicle_positions",
                   help="όνομα του topic (default: vehicle_positions)")
    p.add_argument("--interval", type=int, default=5, metavar="N",
                   help="Ν: δευτερόλεπτα εξομοίωσης ανά κύκλο αποστολής (default: 5)")
    p.add_argument("--speedup", type=float, default=10.0,
                   help="επιτάχυνση ως προς τον πραγματικό χρόνο (default: 10)")
    p.add_argument("--tmax", type=int, default=3600,
                   help="συνολική διάρκεια εξομοίωσης σε δευτερόλεπτα (default: 3600)")
    p.add_argument("--seed", type=int, default=42,
                   help="random seed για αναπαραγωγιμότητα (default: 42)")
    p.add_argument("--include-stopped", action="store_true",
                   help="να σταλούν και τα σταματημένα οχήματα (χωρίς φίλτρο speed > 0)")
    p.add_argument("--retries", type=int, default=5,
                   help="προσπάθειες σύνδεσης στον broker (default: 5)")
    p.add_argument("--backoff", type=int, default=3,
                   help="δευτερόλεπτα αναμονής μεταξύ προσπαθειών (default: 3)")
    return p.parse_args(argv)


if __name__ == "__main__":
    sys.exit(run(parse_args()))
