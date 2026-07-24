# =====================================================================
# Job Spark Structured Streaming — traitement & scoring (CDC §4.7 étape 8)
# =====================================================================
FROM python:3.11-slim

# Installation de Java via default-jre-headless et gestion dynamique du JAVA_HOME
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    procps \
    && rm -rf /var/lib/apt-get/lists/*

# Définition automatique du JAVA_HOME vers l'installation par défaut
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY churn_preprocessing.py model_loader.py streaming_job.py .

RUN mkdir -p /checkpoints /app/model_cache && chmod -R 777 /checkpoints /app/model_cache

ENTRYPOINT ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"]
CMD ["/app/streaming_job.py"]


