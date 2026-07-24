"""
API FastAPI — exposition sécurisée des résultats (CDC §4.7 étape 10, Module 8)

Endpoints :
  POST /token             — authentification, retourne un JWT
  GET  /health             — sonde de santé (health check Docker)
  POST /predict             — prédiction en direct pour un profil client (JWT requis)
  GET  /scores/{customer_id} — dernier score connu d'un client (JWT requis)
  GET  /clients/risque      — top clients à risque (JWT requis)
  GET  /kpis                — indicateurs agrégés temps réel (JWT requis)
  GET  /dashboard           — sert le dashboard HTML (public, lecture seule)
"""
import os
from datetime import timedelta

import pandas as pd
from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

import churn_preprocessing as prep
from auth import authenticate_user, create_access_token, get_current_user
from db import fetch_all, fetch_one
from model_loader import load_model

app = FastAPI(title="API Churn CANAL+", version="1.0",
              description="Exposition sécurisée des scores et KPIs de churn (CDC §4.7)")

# CORS ouvert pour la démo (dashboard HTML tournant dans un navigateur qui
# appelle l'API sur localhost). En prod on restreindra allow_origins à la
# vraie URL du dashboard.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

HF_REPO_ID = os.environ["HF_REPO_ID"]
HF_TOKEN = os.environ.get("HF_TOKEN")
SEUIL_CHURN = float(os.environ.get("SEUIL_CHURN", "0.5"))

# Chargé une fois au démarrage du service (cf. model_loader : cache local
# de secours si le Hub est indisponible, CDC §8.3)
print(f"[api] Chargement du pipeline depuis Hugging Face : {HF_REPO_ID}")
PIPELINE = load_model(HF_REPO_ID, token=HF_TOKEN)
print("[api] Pipeline chargé.")

# Sert le dashboard HTML statique (prototype V2) sous /dashboard/*
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
if os.path.isdir(STATIC_DIR):
    app.mount("/dashboard", StaticFiles(directory=STATIC_DIR, html=True), name="dashboard")


# ---------------------------------------------------------------------
# Schémas
# ---------------------------------------------------------------------
class PredictionRequest(BaseModel):
    gender: str = "Male"
    SeniorCitizen: int = 0
    Partner: str = "No"
    Dependents: str = "No"
    tenure: int = Field(..., ge=0, le=100)
    PhoneService: str = "Yes"
    MultipleLines: str = "No"
    InternetService: str = "Fiber optic"
    OnlineSecurity: str = "No"
    OnlineBackup: str = "No"
    DeviceProtection: str = "No"
    TechSupport: str = "No"
    StreamingTV: str = "No"
    StreamingMovies: str = "No"
    Contract: str = "Month-to-month"
    PaperlessBilling: str = "Yes"
    PaymentMethod: str = "Electronic check"
    MonthlyCharges: float = Field(..., ge=0)
    TotalCharges: float = Field(..., ge=0)


class PredictionResponse(BaseModel):
    churn_score: float
    churn_predicted: bool
    niveau_risque: str
    seuil_applique: float


# ---------------------------------------------------------------------
# Authentification
# ---------------------------------------------------------------------
@app.post("/token")
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=401, detail="Nom d'utilisateur ou mot de passe incorrect")
    token = create_access_token({"sub": user["username"], "role": user["role"]})
    return {"access_token": token, "token_type": "bearer"}


# ---------------------------------------------------------------------
# Santé
# ---------------------------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok"}


# ---------------------------------------------------------------------
# Prédiction en direct (§2.2 : "prédiction en live par client")
# ---------------------------------------------------------------------
@app.post("/predict", response_model=PredictionResponse)
def predict(payload: PredictionRequest, user: dict = Depends(get_current_user)):
    df = pd.DataFrame([payload.model_dump()])
    featured = prep.preprocess(df)
    X = featured.drop(columns=[c for c in ["Churn", "customerID"] if c in featured.columns])

    score = float(PIPELINE.predict_proba(X)[0, 1])
    predicted = score >= SEUIL_CHURN
    niveau = "ELEVE" if score >= 0.70 else "MOYEN" if score >= 0.50 else "FAIBLE"

    return PredictionResponse(
        churn_score=round(score, 4),
        churn_predicted=predicted,
        niveau_risque=niveau,
        seuil_applique=SEUIL_CHURN,
    )


# ---------------------------------------------------------------------
# Consultation des scores existants
# ---------------------------------------------------------------------
@app.get("/scores/{customer_id}")
def get_score(customer_id: str, user: dict = Depends(get_current_user)):
    row = fetch_one(
        """SELECT customer_id, churn_score, churn_predicted, model_version, predicted_ts
           FROM v_dernier_score WHERE customer_id = %s""",
        (customer_id,),
    )
    if not row:
        raise HTTPException(status_code=404, detail="Client non trouvé ou pas encore scoré")
    return row


@app.get("/clients/risque")
def clients_a_risque(limite: int = 50, user: dict = Depends(get_current_user)):
    limite = min(limite, 500)
    return fetch_all("SELECT * FROM v_clients_risque LIMIT %s", (limite,))


@app.get("/kpis")
def kpis(user: dict = Depends(get_current_user)):
    row = fetch_one("SELECT * FROM v_kpis_live")
    return row or {}


# ---------------------------------------------------------------------
# Endpoints analytiques — alimentent les graphiques du dashboard
# ---------------------------------------------------------------------
@app.get("/analytics/etat-base")
def etat_base(user: dict = Depends(get_current_user)):
    """KPIs descriptifs globaux : nb clients, taux churn observé,
    MRR total observé, ancienneté moyenne. Alimentent les 4 cartes du haut."""
    row = fetch_one("""
        SELECT
            count(*)                                              AS clients_en_base,
            round(avg((churn='Yes')::int::numeric), 4)            AS taux_churn_observe,
            count(*) FILTER (WHERE churn='Yes')                   AS nb_resiliations,
            round(sum(monthly_charges)::numeric / 1000000.0, 2)   AS mrr_total_meur,
            round(avg(monthly_charges), 2)                        AS mrr_moyen,
            round(avg(tenure), 1)                                 AS anciennete_moyenne,
            round(percentile_cont(0.5) WITHIN GROUP
                  (ORDER BY tenure)::numeric, 1)                  AS anciennete_mediane
        FROM v_churn_data_dernier_etat
    """)
    return row or {}


@app.get("/analytics/churn-par-contrat")
def churn_par_contrat(user: dict = Depends(get_current_user)):
    return fetch_all("SELECT * FROM v_churn_par_contrat ORDER BY taux_churn DESC")


@app.get("/analytics/churn-par-tenure")
def churn_par_tenure(user: dict = Depends(get_current_user)):
    return fetch_all("SELECT * FROM v_churn_par_tenure ORDER BY tranche")


@app.get("/analytics/churn-par-services")
def churn_par_services(user: dict = Depends(get_current_user)):
    return fetch_all("SELECT * FROM v_churn_par_services ORDER BY nb_services")


@app.get("/analytics/actuals-vs-predictions")
def actuals_vs_predictions(user: dict = Depends(get_current_user)):
    row = fetch_one("SELECT * FROM v_actuals_vs_predictions")
    return row or {}


@app.get("/analytics/cohortes-retention")
def cohortes_retention(user: dict = Depends(get_current_user)):
    return fetch_all("SELECT * FROM v_cohortes_retention ORDER BY mois_cohorte DESC LIMIT 12")


@app.get("/analytics/flux-live")
def flux_live(limite: int = 20, user: dict = Depends(get_current_user)):
    """Derniers évènements ingérés avec leur score — alimente le ticker."""
    limite = min(limite, 50)
    return fetch_all("""
        SELECT
            d.customer_id, d.event_type, d.event_ts,
            m.churn_score
        FROM churn_data d
        LEFT JOIN LATERAL (
            SELECT churn_score FROM churn_model
            WHERE customer_id = d.customer_id
            ORDER BY predicted_ts DESC LIMIT 1
        ) m ON true
        ORDER BY d.ingested_ts DESC
        LIMIT %s
    """, (limite,))
