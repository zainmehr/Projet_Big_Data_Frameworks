# -*- coding: utf-8 -*-
"""
app.py - API REST securisee exposant les 4 datamarts immobiliers.

Lancement (depuis la racine du projet) :
    uvicorn api.app:app --reload

Documentation interactive : http://localhost:8000/docs

Endpoints :
    GET  /                              -> sante de l'API (public)
    POST /auth/login                    -> retourne un token JWT
    GET  /datamarts/prix-par-commune     (JWT)  pagine
    GET  /datamarts/evolution-temporelle (JWT)  pagine
    GET  /datamarts/prix-par-densite     (JWT)  pagine
    GET  /datamarts/segmentation-biens   (JWT)  pagine
"""

import math
import os

from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm

from .auth import authenticate_user, create_access_token, get_current_user
from .database import Database
from .models import PaginatedResponse, Token

# --- Configuration ---
CONFIG_PATH = os.environ.get(
    "API_CONFIG",
    os.path.normpath(
        os.path.join(os.path.dirname(__file__), "..", "config", "config.ini")
    ),
)

db = Database(CONFIG_PATH)

# Map fixe : nom logique d'endpoint -> (table SQL, colonnes de tri).
# Ces valeurs viennent du serveur, jamais de l'utilisateur -> pas d'injection SQL.
# Le tri garantit une pagination deterministe (LIMIT/OFFSET stable).
DATAMARTS = {
    "prix-par-commune":     ("dm_prix_par_commune",     "code_commune, type_local"),
    "evolution-temporelle": ("dm_evolution_temporelle", "annee, mois, type_local"),
    "prix-par-densite":     ("dm_prix_par_densite",     "ordre_tranche, type_local"),
    "segmentation-biens":   ("dm_segmentation_biens",   "segment_id"),
}

app = FastAPI(
    title="API Marche Immobilier Francais",
    description=(
        "Expose les 4 datamarts de la data platform (couche Gold). "
        "Authentification JWT obligatoire sur les endpoints /datamarts."
    ),
    version="1.0.0",
)

# CORS ouvert : pratique pour tester depuis un navigateur ou un autre outil
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------
# ENDPOINT PUBLIC : sante de l'API
# ---------------------------------------------

@app.get("/", tags=["Sante"])
def health():
    """Verifie que l'API repond et liste les datamarts disponibles."""
    return {"status": "ok", "datamarts": list(DATAMARTS.keys())}


# ---------------------------------------------
# AUTHENTIFICATION : obtention du token JWT
# ---------------------------------------------

@app.post("/auth/login", response_model=Token, tags=["Authentification"])
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    """
    Authentifie l'utilisateur et retourne un token JWT.
    Identifiants par defaut : admin / admin (definis dans config.ini).
    """
    if not authenticate_user(form_data.username, form_data.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Identifiants incorrects",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return Token(access_token=create_access_token(form_data.username))


# ---------------------------------------------
# FONCTION UTILITAIRE PARTAGEE
# ---------------------------------------------

def _get_datamart(name, page, page_size):
    """Lit une page d'un datamart et construit la reponse paginee."""
    table, order_by = DATAMARTS[name]
    try:
        rows, total = db.fetch_paginated(table, order_by, page, page_size)
    except Exception as e:
        # La base est injoignable ou la table n'existe pas encore
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Base de donnees indisponible : {}".format(e),
        )
    return PaginatedResponse(
        datamart=table,
        page=page,
        page_size=page_size,
        total_rows=total,
        total_pages=max(1, math.ceil(total / page_size)),
        data=rows,
    )


# ---------------------------------------------
# ENDPOINTS DATAMART (proteges par JWT, pagines)
# ---------------------------------------------

@app.get("/datamarts/prix-par-commune", response_model=PaginatedResponse, tags=["Datamarts"])
def prix_par_commune(
    page: int = Query(1, ge=1, description="Numero de page"),
    page_size: int = Query(50, ge=1, le=1000, description="Lignes par page (max 1000)"),
    user: str = Depends(get_current_user),
):
    """Prix moyen/median au m2 par commune et type de bien."""
    return _get_datamart("prix-par-commune", page, page_size)


@app.get("/datamarts/evolution-temporelle", response_model=PaginatedResponse, tags=["Datamarts"])
def evolution_temporelle(
    page: int = Query(1, ge=1, description="Numero de page"),
    page_size: int = Query(50, ge=1, le=1000, description="Lignes par page (max 1000)"),
    user: str = Depends(get_current_user),
):
    """Evolution mensuelle des prix et volumes de ventes par type de bien."""
    return _get_datamart("evolution-temporelle", page, page_size)


@app.get("/datamarts/prix-par-densite", response_model=PaginatedResponse, tags=["Datamarts"])
def prix_par_densite(
    page: int = Query(1, ge=1, description="Numero de page"),
    page_size: int = Query(50, ge=1, le=1000, description="Lignes par page (max 1000)"),
    user: str = Depends(get_current_user),
):
    """Prix par tranche de population et indicateur de tension du marche."""
    return _get_datamart("prix-par-densite", page, page_size)


@app.get("/datamarts/segmentation-biens", response_model=PaginatedResponse, tags=["Datamarts"])
def segmentation_biens(
    page: int = Query(1, ge=1, description="Numero de page"),
    page_size: int = Query(50, ge=1, le=1000, description="Lignes par page (max 1000)"),
    user: str = Depends(get_current_user),
):
    """Segmentation des biens par surface, pieces et type, avec label de segment."""
    return _get_datamart("segmentation-biens", page, page_size)
