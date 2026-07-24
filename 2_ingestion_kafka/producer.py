#!/usr/bin/env python3
"""
Producer Kafka — simulateur d'évènements clients CANAL+ (CDC §4.7 étape 7)

Principe :
  - Lit le dataset de référence (CanalPlus_Churn_1M.csv)
  - Pour chaque client, génère une SignupDate cohérente avec `tenure`
    (signup_date = date_reference - tenure mois)
  - Émet un évènement initial de type "abonnement" sur le topic dédié
  - Simule un flux continu d'évènements "usage"/"paiement" pour les
    clients actifs
  - Pour les clients dont Churn='Yes', RÉÉMET un évènement avec
    churn_date renseignée (postérieure au signup) — conforme au
    principe append-only de churn_data (jamais d'UPDATE, toujours
    une nouvelle ligne représentant le dernier état connu)

Usage :
    python producer.py --mode replay --rate 50          # rejoue le dataset à 50 évts/s
    python producer.py --mode continuous --rate 20       # flux continu perpétuel
    python producer.py --mode replay --rate 50 --limit 5000   # test sur 5000 clients
"""
import argparse
import csv
import json
import logging
import random
import sys
import time
from datetime import datetime, timedelta, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("producer")

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------
KAFKA_BOOTSTRAP = None   # défini via CLI/env, ex. "localhost:29092" ou "kafka:9092"
TOPICS = {
    "abonnement": "abonnements",
    "usage": "usages",
    "paiement": "paiements",
    "resiliation": "abonnements",   # la résiliation est un évènement d'abonnement (réémission)
}
DATE_REFERENCE = datetime.now(timezone.utc)   # "aujourd'hui" simulé, ancre des calculs de dates

CSV_COLUMNS_MAP = {
    # colonne CSV -> clé JSON attendue par Spark / churn_preprocessing.py
    "customerID": "customer_id",
    "gender": "gender",
    "SeniorCitizen": "senior_citizen",
    "Partner": "partner",
    "Dependents": "dependents",
    "tenure": "tenure",
    "PhoneService": "phone_service",
    "MultipleLines": "multiple_lines",
    "InternetService": "internet_service",
    "OnlineSecurity": "online_security",
    "OnlineBackup": "online_backup",
    "DeviceProtection": "device_protection",
    "TechSupport": "tech_support",
    "StreamingTV": "streaming_tv",
    "StreamingMovies": "streaming_movies",
    "Contract": "contract",
    "PaperlessBilling": "paperless_billing",
    "PaymentMethod": "payment_method",
    "MonthlyCharges": "monthly_charges",
    "TotalCharges": "total_charges",
    "Churn": "churn",
}


# ---------------------------------------------------------------------
# Génération de dates cohérentes
# ---------------------------------------------------------------------
def compute_signup_date(tenure_months: int) -> datetime:
    """SignupDate = date de référence - tenure mois (+/- quelques jours de bruit réaliste)."""
    jitter_days = random.randint(-10, 10)
    return DATE_REFERENCE - timedelta(days=tenure_months * 30 + jitter_days)


def compute_churn_date(signup_date: datetime, tenure_months: int) -> datetime:
    """
    ChurnDate : postérieure au signup, cohérente avec l'ancienneté déclarée.
    On place la résiliation dans le dernier tiers de la période d'ancienneté
    (reflète un désengagement progressif plutôt qu'un abandon immédiat après signup).
    """
    duree_totale_jours = max(tenure_months * 30, 5)
    debut_fenetre = int(duree_totale_jours * 0.6)
    jour_churn = random.randint(debut_fenetre, duree_totale_jours)
    churn_date = signup_date + timedelta(days=jour_churn)
    # Ne jamais dépasser la date de référence (le futur n'existe pas encore)
    return min(churn_date, DATE_REFERENCE - timedelta(days=1))


# ---------------------------------------------------------------------
# Construction des évènements
# ---------------------------------------------------------------------
def build_base_record(row: dict) -> dict:
    """Convertit une ligne CSV en dict aux clés normalisées, avec dates calculées."""
    record = {}
    for csv_col, json_key in CSV_COLUMNS_MAP.items():
        record[json_key] = row.get(csv_col, "").strip()

    # Typage numérique
    try:
        tenure = int(float(record["tenure"] or 0))
    except ValueError:
        tenure = 0
    record["tenure"] = tenure
    record["senior_citizen"] = int(float(record["senior_citizen"] or 0))
    try:
        record["monthly_charges"] = float(record["monthly_charges"] or 0)
    except ValueError:
        record["monthly_charges"] = 0.0
    try:
        record["total_charges"] = float(record["total_charges"] or 0)
    except ValueError:
        record["total_charges"] = None  # cohérent avec les vides du dataset réel

    signup = compute_signup_date(tenure)
    record["signup_date"] = signup.date().isoformat()
    record["churn_date"] = None

    if record["churn"] == "Yes":
        churn_dt = compute_churn_date(signup, tenure)
        record["churn_date"] = churn_dt.date().isoformat()

    return record


def make_event(record: dict, event_type: str) -> dict:
    """Enveloppe un évènement avec ses métadonnées de traçabilité (event_ts, event_type)."""
    evt = dict(record)
    evt["event_type"] = event_type
    evt["event_ts"] = datetime.now(timezone.utc).isoformat()
    evt["source"] = "kafka_simulator"
    return evt


# ---------------------------------------------------------------------
# Envoi Kafka
# ---------------------------------------------------------------------
def get_producer(bootstrap_servers: str):
    from confluent_kafka import Producer

    conf = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": "churn-producer-simulator",
        "acks": "all",
        "linger.ms": 20,          # regroupe les envois -> meilleur débit
        "compression.type": "lz4",
    }
    return Producer(conf)


def delivery_callback(err, msg):
    if err is not None:
        log.error("Échec de livraison : %s", err)


def send_event(producer, topic: str, key: str, payload: dict):
    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        callback=delivery_callback,
    )


# ---------------------------------------------------------------------
# Modes de simulation
# ---------------------------------------------------------------------
def run_replay(producer, csv_path: str, rate: float, limit: int | None):
    """
    Rejoue le dataset une fois : émet l'évènement initial 'abonnement' pour
    chaque client, puis, pour les résiliants, réémet un évènement de
    résiliation avec churn_date renseignée (append-only : nouvelle ligne).
    """
    delay = 1.0 / rate if rate > 0 else 0
    sent, churned = 0, 0

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if limit and i >= limit:
                break

            record = build_base_record(row)
            customer_id = record["customer_id"]

            # 1) Évènement initial : abonnement (état "actif", churn_date=NULL)
            initial = dict(record)
            initial["churn"] = "No"          # au moment du signup, le client est actif
            initial["churn_date"] = None
            evt = make_event(initial, "abonnement")
            send_event(producer, TOPICS["abonnement"], customer_id, evt)
            sent += 1

            # 2) Si le client a churné : réémission avec l'état final
            if record["churn"] == "Yes":
                evt_resil = make_event(record, "resiliation")
                send_event(producer, TOPICS["resiliation"], customer_id, evt_resil)
                sent += 1
                churned += 1

            if sent % 500 == 0:
                producer.poll(0)   # traite les callbacks de livraison en attente
                log.info("Envoyés : %d évènements (%d résiliations)", sent, churned)

            if delay:
                time.sleep(delay)

    producer.flush(30)
    log.info("Replay terminé. Total : %d évènements, %d résiliations.", sent, churned)


def run_continuous(producer, csv_path: str, rate: float):
    """
    Flux continu perpétuel : boucle sur le dataset, émet des évènements
    'usage' et 'paiement' pour des clients aléatoires (simule l'activité
    normale), avec une petite probabilité d'évènement 'resiliation'.
    """
    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    log.info("Dataset chargé en mémoire : %d clients. Démarrage du flux continu...", len(rows))

    delay = 1.0 / rate if rate > 0 else 0
    sent = 0
    try:
        while True:
            row = random.choice(rows)
            record = build_base_record(row)
            customer_id = record["customer_id"]

            roll = random.random()
            if roll < 0.05 and record["churn"] == "Yes":
                # Résiliation simulée en direct
                evt = make_event(record, "resiliation")
                send_event(producer, TOPICS["resiliation"], customer_id, evt)
            elif roll < 0.55:
                evt = make_event(record, "usage")
                send_event(producer, TOPICS["usage"], customer_id, evt)
            else:
                evt = make_event(record, "paiement")
                send_event(producer, TOPICS["paiement"], customer_id, evt)

            sent += 1
            if sent % 200 == 0:
                producer.poll(0)
                log.info("Flux continu : %d évènements envoyés", sent)

            if delay:
                time.sleep(delay)
    except KeyboardInterrupt:
        log.info("Arrêt demandé (Ctrl+C). Vidage du buffer...")
        producer.flush(30)
        log.info("Flux arrêté proprement. Total : %d évènements.", sent)


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Producer Kafka — simulateur churn CANAL+")
    parser.add_argument("--bootstrap", default="localhost:29092",
                        help="Adresse du broker Kafka (défaut: localhost:29092, hors Docker)")
    parser.add_argument("--csv", default="/data/CanalPlus_Churn_1M.csv",
                        help="Chemin du dataset source")
    parser.add_argument("--mode", choices=["replay", "continuous"], default="replay",
                        help="replay: rejoue le dataset une fois | continuous: flux perpétuel")
    parser.add_argument("--rate", type=float, default=50,
                        help="Évènements par seconde (défaut: 50)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Limite le nombre de clients traités (mode replay, utile pour tester)")
    args = parser.parse_args()

    log.info("Producer démarré | broker=%s | mode=%s | rate=%s evt/s | csv=%s",
             args.bootstrap, args.mode, args.rate, args.csv)

    producer = get_producer(args.bootstrap)

    if args.mode == "replay":
        run_replay(producer, args.csv, args.rate, args.limit)
    else:
        run_continuous(producer, args.csv, args.rate)


if __name__ == "__main__":
    main()
