"""Authentification JWT/Token — CDC §4.7 étape 10, §8.1.

Les comptes sont stockés dans la table api_users (init.sql), avec un
hash bcrypt du mot de passe — jamais de mot de passe en clair (§8.2).
"""
import os
from datetime import datetime, timedelta, timezone

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from passlib.context import CryptContext

from db import fetch_one

SECRET_KEY = os.environ["JWT_SECRET_KEY"]
ALGORITHM = os.environ.get("JWT_ALGORITHM", "HS256")
EXPIRE_MINUTES = int(os.environ.get("JWT_EXPIRE_MINUTES", "60"))

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


def hash_password(plain: str) -> str:
    return pwd_context.hash(plain)


def authenticate_user(username: str, password: str):
    user = fetch_one(
        "SELECT id, username, password_hash, role, actif FROM api_users WHERE username = %s",
        (username,),
    )
    if not user or not user["actif"] or not verify_password(password, user["password_hash"]):
        return None
    return user


def create_access_token(data: dict) -> str:
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(minutes=EXPIRE_MINUTES)
    to_encode["exp"] = expire
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def get_current_user(token: str = Depends(oauth2_scheme)) -> dict:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Identifiants invalides ou expirés",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    return {"username": username, "role": payload.get("role", "lecteur")}
