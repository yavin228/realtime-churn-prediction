"""Chargement résilient du modèle churn depuis Hugging Face Hub.

Cache local de secours en cas d'indisponibilité du Hub (CDC §8.3).
"""
import os
import shutil
import joblib
from huggingface_hub import hf_hub_download

PIPELINE_FILENAME = "churn_pipeline.joblib"


def load_model(repo_id, token=None, local_cache_dir="/app/model_cache"):
    os.makedirs(local_cache_dir, exist_ok=True)
    cached = os.path.join(local_cache_dir, PIPELINE_FILENAME)
    try:
        path = hf_hub_download(repo_id=repo_id, filename=PIPELINE_FILENAME, token=token)
        shutil.copy(path, cached)
        return joblib.load(path)
    except Exception as e:
        if os.path.exists(cached):
            print(f"[model_loader] Hub indisponible ({e}). Chargement depuis le cache local.")
            return joblib.load(cached)
        raise RuntimeError(f"Modèle introuvable (Hub KO et pas de cache) : {e}")
