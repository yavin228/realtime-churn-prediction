"""
Job Spark Structured Streaming — traitement & scoring temps réel (CDC §4.7 étape 8)

Flux :
  1) Consomme les 3 topics Kafka (abonnements, usages, paiements)
  2) Pour chaque micro-batch : renomme les champs vers les noms de colonnes
     attendus par le pipeline entraîné (CamelCase, identique au CSV
     d'entraînement), applique churn_preprocessing.preprocess()
  3) Charge le pipeline complet (prétraitement + modèle) publié sur
     Hugging Face Hub via model_loader.load_model()
  4) Calcule le score de churn (predict_proba)
  5) Écrit en parallèle :
       - les évènements bruts dans churn_data (traçabilité, analyse)
       - les scores dans churn_model (prédiction)

Déclenchement : micro-batch toutes les TRIGGER_SECONDS secondes
(CDC §8.1 : objectif de latence de quelques secondes).
"""
import json
import os

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

import churn_preprocessing as prep
from model_loader import load_model

# ---------------------------------------------------------------------
# Configuration (variables d'environnement, cf. docker-compose.yml)
# ---------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
KAFKA_TOPICS = os.environ.get("KAFKA_TOPICS", "abonnements,usages,paiements")

PG_HOST = os.environ["POSTGRES_HOST"]
PG_PORT = os.environ.get("POSTGRES_PORT", "5432")
PG_DB = os.environ["POSTGRES_DB"]
PG_USER = os.environ["POSTGRES_USER"]
PG_PASSWORD = os.environ["POSTGRES_PASSWORD"]

HF_REPO_ID = os.environ["HF_REPO_ID"]
HF_TOKEN = os.environ.get("HF_TOKEN")

SEUIL_CHURN = float(os.environ.get("SEUIL_CHURN", "0.5"))
TRIGGER_SECONDS = int(os.environ.get("TRIGGER_SECONDS", "10"))
MODEL_VERSION = os.environ.get("MODEL_VERSION", "1")

# ---------------------------------------------------------------------
# Renommage : les topics Kafka (produits par producer.py) utilisent des
# clés snake_case alignées sur les colonnes PostgreSQL. Le pipeline
# entraîné (MLflow / Hugging Face) attend les noms de colonnes ORIGINAUX
# du CSV d'entraînement (CamelCase). Ce mapping évite un training-serving
# skew silencieux : sans lui, le ColumnTransformer ne retrouverait pas
# ses colonnes et échouerait (ou pire, se tairait avec `remainder`).
# ---------------------------------------------------------------------
SNAKE_TO_TRAINING_NAMES = {
    "customer_id": "customerID",
    "gender": "gender",
    "senior_citizen": "SeniorCitizen",
    "partner": "Partner",
    "dependents": "Dependents",
    "tenure": "tenure",
    "phone_service": "PhoneService",
    "multiple_lines": "MultipleLines",
    "internet_service": "InternetService",
    "online_security": "OnlineSecurity",
    "online_backup": "OnlineBackup",
    "device_protection": "DeviceProtection",
    "tech_support": "TechSupport",
    "streaming_tv": "StreamingTV",
    "streaming_movies": "StreamingMovies",
    "contract": "Contract",
    "paperless_billing": "PaperlessBilling",
    "payment_method": "PaymentMethod",
    "monthly_charges": "MonthlyCharges",
    "total_charges": "TotalCharges",
    "churn": "Churn",
}

EVENT_SCHEMA = StructType([
    StructField("customer_id", StringType()),
    StructField("gender", StringType()),
    StructField("senior_citizen", IntegerType()),
    StructField("partner", StringType()),
    StructField("dependents", StringType()),
    StructField("tenure", IntegerType()),
    StructField("phone_service", StringType()),
    StructField("multiple_lines", StringType()),
    StructField("internet_service", StringType()),
    StructField("online_security", StringType()),
    StructField("online_backup", StringType()),
    StructField("device_protection", StringType()),
    StructField("tech_support", StringType()),
    StructField("streaming_tv", StringType()),
    StructField("streaming_movies", StringType()),
    StructField("contract", StringType()),
    StructField("paperless_billing", StringType()),
    StructField("payment_method", StringType()),
    StructField("monthly_charges", DoubleType()),
    StructField("total_charges", DoubleType()),
    StructField("churn", StringType()),
    StructField("signup_date", StringType()),   # ISO string, casté en DATE côté SQL
    StructField("churn_date", StringType()),
    StructField("event_type", StringType()),
    StructField("event_ts", StringType()),
    StructField("source", StringType()),
])

# ---------------------------------------------------------------------
# Chargement du modèle — une seule fois, réutilisé à chaque micro-batch
# ---------------------------------------------------------------------
print(f"[spark_job] Chargement du pipeline depuis Hugging Face : {HF_REPO_ID}")
PIPELINE = load_model(HF_REPO_ID, token=HF_TOKEN)
print("[spark_job] Pipeline chargé :", PIPELINE.named_steps.get("model", PIPELINE.steps[-1][1]).__class__.__name__)


# ---------------------------------------------------------------------
# Connexion PostgreSQL (une connexion par micro-batch, simple et robuste
# à cette échelle ; un pool serait préférable en très forte charge)
# ---------------------------------------------------------------------
def get_pg_conn():
    return psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                             user=PG_USER, password=PG_PASSWORD)


def insert_churn_data(conn, pdf: pd.DataFrame):
    """Écrit les évènements bruts (append-only) dans churn_data."""
    cols = ["customer_id", "gender", "senior_citizen", "partner", "dependents", "tenure",
            "phone_service", "multiple_lines", "internet_service", "online_security",
            "online_backup", "device_protection", "tech_support", "streaming_tv",
            "streaming_movies", "contract", "paperless_billing", "payment_method",
            "monthly_charges", "total_charges", "churn", "signup_date", "churn_date",
            "event_type", "event_ts", "source"]
    rows = [tuple(None if pd.isna(v) else v for v in row) for row in pdf[cols].itertuples(index=False)]
    with conn.cursor() as cur:
        execute_values(cur, f"INSERT INTO churn_data ({', '.join(cols)}) VALUES %s", rows)
    conn.commit()


def insert_churn_model(conn, customer_ids, scores, event_ts):
    """Écrit les scores de prédiction (append-only) dans churn_model."""
    rows = [
        (cid, float(score), int(score >= SEUIL_CHURN), SEUIL_CHURN,
         "churn_model_canalplus", MODEL_VERSION, ts)
        for cid, score, ts in zip(customer_ids, scores, event_ts)
    ]
    cols = ["customer_id", "churn_score", "churn_predicted", "seuil_applique",
            "model_name", "model_version", "event_ts"]
    with conn.cursor() as cur:
        execute_values(cur, f"INSERT INTO churn_model ({', '.join(cols)}) VALUES %s", rows)
    conn.commit()


# ---------------------------------------------------------------------
# Traitement d'un micro-batch
# ---------------------------------------------------------------------
def process_batch(batch_df, batch_id: int):
    if batch_df.rdd.isEmpty():
        return

    pdf = batch_df.toPandas()
    n = len(pdf)

    conn = get_pg_conn()
    try:
        # 1) Persistance des évènements bruts (traçabilité, alimente les
        #    vues descriptives/temporelles côté Power BI)
        insert_churn_data(conn, pdf)

        # 2) Scoring : renommage vers les noms de colonnes d'entraînement,
        #    puis préprocessing identique au notebook Colab.
        #    ⚠️ On retire explicitement les colonnes techniques (dates,
        #    métadonnées Kafka) qui n'existaient PAS au moment du fit.
        #    Le ColumnTransformer utilise remainder="passthrough" : des
        #    colonnes en trop provoqueraient une erreur de shape (nombre
        #    de features différent) ou un résultat faux silencieux.
        renamed = pdf.rename(columns=SNAKE_TO_TRAINING_NAMES)
        featured = prep.preprocess(renamed)
        NON_FEATURE_COLS = ["Churn", "customerID", "signup_date", "churn_date",
                            "event_type", "event_ts", "source"]
        X = featured.drop(columns=[c for c in NON_FEATURE_COLS if c in featured.columns])

        scores = PIPELINE.predict_proba(X)[:, 1]
        insert_churn_model(conn, pdf["customer_id"].tolist(), scores, pdf["event_ts"].tolist())

        n_risque = int((scores >= SEUIL_CHURN).sum())
        print(f"[batch {batch_id}] {n} évènements traités | {n_risque} scorés à risque "
              f"(seuil {SEUIL_CHURN}) | score moyen {scores.mean():.3f}")
    except Exception as e:
        conn.rollback()
        print(f"[batch {batch_id}] ERREUR : {e}")
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------
# Démarrage du streaming
# ---------------------------------------------------------------------
def main():
    spark = (SparkSession.builder
             .appName("churn-canalplus-streaming")
             .master("local[*]")
             .config("spark.driver.host", "localhost")
             .config("spark.driver.bindAddress", "0.0.0.0")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", KAFKA_TOPICS)
           .option("startingOffsets", "latest")
           .option("failOnDataLoss", "false")
           .load())

    events = (raw
              .selectExpr("CAST(value AS STRING) AS json_str")
              .select(from_json(col("json_str"), EVENT_SCHEMA).alias("evt"))
              .select("evt.*"))

    query = (events.writeStream
             .foreachBatch(process_batch)
             .option("checkpointLocation", "/checkpoints/churn_streaming")
             .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
             .start())

    print(f"[spark_job] Streaming démarré | trigger={TRIGGER_SECONDS}s | topics={KAFKA_TOPICS}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
