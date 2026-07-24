"""Prétraitement + feature engineering du dataset churn CANAL+.

IMPORTANT : ce module est identique à celui publié sur Hugging Face
(yavin228/churn-canalplus) et utilisé à l'entraînement (Google Colab).
Le garder synchronisé ici évite tout training-serving skew (CDC §8.1).
En pratique, model_loader.py télécharge la version faisant foi depuis
le Hub ; cette copie locale sert de repli si le Hub est injoignable.
"""
import pandas as pd

SERVICE_COLS = ["OnlineSecurity", "OnlineBackup", "DeviceProtection",
                "TechSupport", "StreamingTV", "StreamingMovies"]
TARGET = "Churn"
ID_COL = "customerID"


def clean_data(df):
    df = df.copy()
    df.columns = df.columns.str.strip()
    if ID_COL in df.columns:
        df = df.drop_duplicates(subset=ID_COL)
    if "TotalCharges" in df.columns:
        df["TotalCharges"] = pd.to_numeric(df["TotalCharges"], errors="coerce")
    return df


def add_engineered_features(df):
    df = df.copy()
    if "tenure" in df.columns:
        df["NewClient"] = (df["tenure"] < 6).astype(int)
    present = [c for c in SERVICE_COLS if c in df.columns]
    if present:
        df["NbServices"] = (df[present] == "Yes").sum(axis=1)
    return df


def preprocess(df):
    """Nettoyage + feature engineering — identique en développement et en production."""
    return add_engineered_features(clean_data(df))
