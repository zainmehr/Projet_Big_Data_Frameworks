# -*- coding: utf-8 -*-
"""
auth.py - Securite de l'API via JWT (JSON Web Token).

Flux :
  1. L'utilisateur poste ses identifiants sur /auth/login
  2. authenticate_user() les verifie
  3. create_access_token() genere un JWT signe, valable X minutes
  4. Chaque endpoint protege depend de get_current_user(), qui decode
     et verifie le token presente dans l'en-tete Authorization.
"""

import configparser
import os
from datetime import datetime, timedelta, timezone

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer

# --- Chargement de la configuration ([api]) ---
_CONFIG_PATH = os.environ.get(
    "API_CONFIG",
    os.path.normpath(
        os.path.join(os.path.dirname(__file__), "..", "config", "config.ini")
    ),
)

_config = configparser.ConfigParser()
_config.read(_CONFIG_PATH)
_api = _config["api"]

SECRET_KEY = _api.get("secret_key", "change_me_in_production")
ALGORITHM = _api.get("algorithm", "HS256")
EXPIRE_MINUTES = int(_api.get("token_expire_minutes", "60"))
LOGIN_USER = _api.get("login_user", "admin")
LOGIN_PASSWORD = _api.get("login_password", "admin")

# tokenUrl = endpoint ou Swagger UI ira chercher le token (bouton "Authorize")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/login")


def authenticate_user(username, password):
    """Verifie les identifiants fournis a la connexion."""
    return username == LOGIN_USER and password == LOGIN_PASSWORD


def create_access_token(username):
    """Genere un JWT signe contenant l'utilisateur et une date d'expiration."""
    expire = datetime.now(timezone.utc) + timedelta(minutes=EXPIRE_MINUTES)
    payload = {"sub": username, "exp": expire}
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def get_current_user(token: str = Depends(oauth2_scheme)):
    """
    Dependance FastAPI : decode et valide le token JWT.
    Leve une 401 si le token est absent, invalide ou expire.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Token invalide ou expire",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if username is None:
            raise credentials_exception
        return username
    except jwt.PyJWTError:
        raise credentials_exception
