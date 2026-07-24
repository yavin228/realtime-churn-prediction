#!/usr/bin/env bash
# =====================================================================
# Création des topics Kafka — CDC §4.7 étape 7
# Topics dédiés : abonnements, usages, paiements
# Chaque évènement publié correspond à une ligne future dans churn_data
# (colonne event_type). Usage (Kafka démarré) :
#   bash 2_ingestion_kafka/create_topics.sh
# =====================================================================
set -euo pipefail

BROKER_CONTAINER="churn_kafka"
BOOTSTRAP="localhost:9092"          # listener interne, exécuté DANS le conteneur
PARTITIONS=3                        # dimensionnement (CDC §8.3, mitigation latence)
REPLICATION=1                       # broker unique en Phase 1 (VPS mono-nœud)

TOPICS=("abonnements" "usages" "paiements")

echo "Création des topics (partitions=${PARTITIONS}, replication=${REPLICATION})..."
for topic in "${TOPICS[@]}"; do
  docker exec "${BROKER_CONTAINER}" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists \
    --topic "${topic}" \
    --partitions "${PARTITIONS}" \
    --replication-factor "${REPLICATION}" \
    --config retention.ms=604800000     # 7 jours, cf. docker-compose

  echo "  -> ${topic} OK"
done

echo ""
echo "Topics présents sur le broker :"
docker exec "${BROKER_CONTAINER}" /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP}" --list

echo ""
echo "Détail des partitions :"
for topic in "${TOPICS[@]}"; do
  docker exec "${BROKER_CONTAINER}" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${BOOTSTRAP}" --describe --topic "${topic}"
done
