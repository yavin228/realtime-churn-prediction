# =====================================================================
# Producer Kafka — simulateur d'évènements churn CANAL+
# =====================================================================
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

# Le dataset est monté en volume (voir docker-compose.yml), pas copié dans
# l'image : évite d'alourdir l'image et permet de changer de dataset sans
# rebuild.
ENTRYPOINT ["python", "producer.py"]
CMD ["--bootstrap", "kafka:9092", "--csv", "/data/CanalPlus_Churn_1M.csv", "--mode", "continuous", "--rate", "20"]
