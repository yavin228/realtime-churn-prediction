# Realtime Churn Prediction - CANAL+ Group

Système de prédiction du churn client en **temps réel** développé lors de mon stage
à **CANAL+ Group ** (Licence Professionnelle Intelligence Artificielle & Big Data,
ESGIS 2025-2026). Architecture end-to-end conforme au cahier des charges §4.7 :
ingestion Kafka → traitement Spark Structured Streaming → persistance PostgreSQL →
exposition sécurisée FastAPI + Dashboard temps réel.

![Python](https://img.shields.io/badge/python-3.11-blue.svg)
![Docker](https://img.shields.io/badge/docker-compose-blue.svg)
![Kafka](https://img.shields.io/badge/kafka-3.7-orange.svg)
![Spark](https://img.shields.io/badge/spark-3.5-orange.svg)
![PostgreSQL](https://img.shields.io/badge/postgresql-16-blue.svg)
![FastAPI](https://img.shields.io/badge/fastapi-0.115-green.svg)
![License](https://img.shields.io/badge/license-Academic-lightgrey.svg)

---

## Architecture
┌──────────┐ ┌───────┐ ┌───────────────┐ ┌────────────┐ ┌───────────────┐
│ Producer │───▶│ Kafka │───▶│ Spark │───▶│ PostgreSQL │───▶│ FastAPI + │
│ (simulé) │ │ 3 top │ │ Streaming │ │ │ │ Dashboard │
│ ~20 e/s │ │ KRaft │ │ + modèle HF │ │ churn_data │ │ /predict │
└──────────┘ └───────┘ │ Trigger 10s │ │ churn_model│ │ /kpis /token │
└───────────────┘ │ + 12 vues │ └───────────────┘
└──────┬─────┘ │
│ │
▼ ▼
Power BI Direct Dashboard HTML
Query (5 pages, JWT)

Le pipeline s'appuie sur le modèle **`yavin228/churn-canalplus`** (Régression Logistique,
AUC 0.66, Recall 0.64) publié sur Hugging Face Hub, entraîné sur un dataset
CANAL+ synthétique d'1 million de clients (cf. architecture de développement).

---

## Table des matières

- [Prérequis](#prérequis)
- [Démarrage rapide](#démarrage-rapide)
- [Structure du projet](#structure-du-projet)
- [Utilisation](#utilisation)
- [API](#api)
- [Dashboard](#dashboard)
- [Connexion Power BI](#connexion-power-bi)
- [Décisions techniques](#décisions-techniques)
- [Auteur](#auteur)

---

## Prérequis

- **Docker** & **Docker Compose v2**
- **8 Go de RAM** disponibles (Kafka + Spark + PostgreSQL + API)
- **Dataset** `CanalPlus_Churn_1M.csv` (1M lignes, 21 variables + `SignupDate`/`ChurnDate`)
- Compte **Hugging Face** avec token d'accès au dépôt `yavin228/churn-canalplus` (privé)

---

## Démarrage rapide

```bash
# 1) Cloner le dépôt
git clone https://github.com/yavin228/realtime-churn-prediction.git
cd realtime-churn-prediction

# 2) Configurer les secrets
cp .env.example .env
nano .env    # renseigner POSTGRES_PASSWORD, HF_TOKEN, JWT_SECRET_KEY

# 3) Placer le dataset (fourni séparément)
cp /chemin/vers/CanalPlus_Churn_1M.csv data/

# 4) Démarrer l'infrastructure de base
docker compose up -d kafka postgres
docker compose ps                      # attendre "healthy" sur les deux

# 5) Créer les topics Kafka
bash 2_ingestion_kafka/create_topics.sh

# 6) Démarrer le traitement + l'API
docker compose up -d --build spark api

# 7) Lancer le producer (flux continu)
docker compose --profile producer up -d producer

# 8) Ouvrir le dashboard
# → http://127.0.0.1:8000/dashboard/
```

---

## Structure du projet



realtime-churn-prediction/
├── docker-compose.yml # orchestration des 5 services
├── .env.example # template des secrets
├── 2_ingestion_kafka/
│ ├── create_topics.sh # création des 3 topics
│ ├── producer.py # simulateur d'évènements clients
│ ├── Dockerfile
│ └── requirements.txt
├── 3_traitement_spark/
│ ├── streaming_job.py # job Spark Structured Streaming
│ ├── churn_preprocessing.py # préprocessing identique à l'entraînement
│ ├── model_loader.py # chargement HF résilient (cache local)
│ ├── Dockerfile
│ └── requirements.txt
├── 4_stockage_postgresql/
│ └── init.sql # schéma complet (2 tables + 12 vues)
├── 5_services_dashboard/api/
│ ├── main.py # FastAPI (JWT + 12 routes)
│ ├── auth.py # authentification bcrypt + JWT
│ ├── db.py # helpers PostgreSQL
│ ├── static/index.html # dashboard HTML 5 pages
│ ├── Dockerfile
│ └── requirements.txt
└── data/ # dataset (non versionné, cf. .gitignore)

---

## Utilisation

### Vérifier que tout tourne

```bash
docker compose ps
# → 5 conteneurs Up : kafka, postgres, spark, api, producer

# Compter les évènements ingérés
docker exec -it churn_postgres psql -U churn_user -d churn_db \
  -c "SELECT COUNT(*) FROM churn_data;"

# Vérifier les micro-batchs Spark
docker compose logs --tail 5 spark | grep batch
```

### Créer un compte API

Générer un hash bcrypt puis l'insérer dans `api_users` :

```bash
docker exec -it churn_api python -c \
  "from passlib.context import CryptContext; \
   print(CryptContext(schemes=['bcrypt']).hash('MonMotDePasseFort'))"

docker exec -it churn_postgres psql -U churn_user -d churn_db -c \
  "INSERT INTO api_users (username, password_hash, role, actif) \
   VALUES ('nick', '<HASH_BCRYPT>', 'admin', true);"
```

### Obtenir un token JWT

```bash
curl -X POST http://localhost:8000/token \
  -d "username=nick&password=MonMotDePasseFort"
# → {"access_token": "eyJ...", "token_type": "bearer"}
```

---

## API

Documentation interactive **Swagger** : http://localhost:8000/docs

| Endpoint | Méthode | Description |
|---|---|---|
| `/health` | GET | Sonde de santé (public) |
| `/token` | POST | Obtenir un JWT (username/password) |
| `/predict` | POST | Prédiction en direct pour un profil (JWT) |
| `/scores/{customer_id}` | GET | Dernier score connu (JWT) |
| `/clients/risque` | GET | Top clients à risque (JWT) |
| `/kpis` | GET | KPIs agrégés temps réel (JWT) |
| `/analytics/etat-base` | GET | Statistiques descriptives globales (JWT) |
| `/analytics/churn-par-contrat` | GET | Taux de churn par type de contrat (JWT) |
| `/analytics/churn-par-tenure` | GET | Distribution par ancienneté (JWT) |
| `/analytics/churn-par-services` | GET | Effet des services souscrits (JWT) |
| `/analytics/actuals-vs-predictions` | GET | Vérité terrain vs prédiction (JWT) |
| `/analytics/flux-live` | GET | Derniers évènements ingérés (JWT) |

---

## Dashboard

Dashboard HTML premium **5 pages** servi par FastAPI sur http://localhost:8000/dashboard/

- **Login sécurisé** via JWT (modale au chargement)
- **KPIs live** rafraîchis toutes les 15 secondes
- **Ticker temps réel** des évènements ingérés par Kafka
- **Palette premium CANAL+** (noir, or `#F2C14E`, rouge `#F0553B`)
- **Chart.js** pour tous les visuels interactifs
- **5 pages** : Vue d'ensemble, Segments clients, Prescriptif · Actions, Temps réel, Simulateur & Projection

Note : ce dashboard est **une démonstration technique**. La solution de production
utilisée par les équipes CANAL+ Marketing est **Power BI** (cf. section suivante).

---

## Connexion Power BI

PostgreSQL expose 12 vues optimisées pour Direct Query :

| Vue | Contenu |
|---|---|
| `v_analyse_complete` | Jointure profil × prédiction (base de tout) |
| `v_clients_risque` | Top clients à risque avec MRR |
| `v_kpis_live` | KPIs 24h |
| `v_actuals_vs_predictions` | Actual vs Predicted (accuracy live) |
| `v_churn_par_contrat` | Répartition par type de contrat |
| `v_churn_par_tenure` | Distribution par ancienneté |
| `v_churn_par_services` | Effet du nombre de services |
| `v_cohortes_retention` | Cohortes d'inscription mensuelles |
| `v_velocite_churn` | Délai réel avant résiliation par contrat |
| `v_tendance_churn_hebdo` | Saisonnalité hebdomadaire |
| `v_dernier_score` | Dernier score connu par client |
| `v_churn_data_dernier_etat` | Dernier profil connu par client |

**Connexion Power BI Desktop** :
- Serveur : `localhost` (ou IP du VPS)
- Port : `5432`
- Base : `churn_db`
- Mode : **DirectQuery** (recommandé pour le temps réel)
- Authentification : Base (`churn_user` / mot de passe défini dans `.env`)

---

## Décisions techniques

- **Append-only** : `churn_data` et `churn_model` ne sont jamais mises à jour, seulement complétées. Le "dernier état" d'un client est reconstitué par vue (`DISTINCT ON ... ORDER BY ingested_ts DESC`). Absorbe le débit du streaming sans verrous.
- **Renommage snake_case ↔ CamelCase** : les topics Kafka et PostgreSQL utilisent des noms snake_case (`monthly_charges`), tandis que le pipeline entraîné attend les noms d'entraînement (`MonthlyCharges`). Le job Spark fait la traduction avant scoring pour éviter tout training-serving skew.
- **Micro-batch 10s** : `TRIGGER_SECONDS=10` — bon compromis latence/débit, respecte l'objectif CDC §8.1 ("latence de quelques secondes").
- **Résilience modèle** : `model_loader.py` télécharge le pipeline depuis Hugging Face au démarrage et le recopie dans un volume partagé. Si le Hub est indisponible au redémarrage, le cache local est utilisé (CDC §8.3).
- **Sécurité** : JWT avec expiration 60 min, hash bcrypt pour les mots de passe stockés, CORS configurable, aucun secret en dur dans le code.

---

## Auteur

**Yavin Kokouvi MITEKOR** — Étudiant Licence Professionnelle IA & Big Data, ESGIS 2025-2026
Analyst Marketing, CANAL+ Group

- 🤗 Hugging Face : [`yavin228`](https://huggingface.co/yavin228)
- 🐙 GitHub : [`yavin228`](https://github.com/yavin228)
- 🎓 Encadrement : Jimmy HUNLEDE — Chargé Renewal, CANAL+ Group Togo

---

## Licence

Projet académique développé dans le cadre d'un stage de Licence Professionnelle.
Usage restreint à des fins pédagogiques et demonstratives.
Le modèle et le dataset restent la propriété de CANAL+ Group.
