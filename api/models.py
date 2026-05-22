# -*- coding: utf-8 -*-
"""
models.py - Schemas Pydantic utilises par l'API.

Le modele de reponse est volontairement generique (data: liste de dict) :
les datamarts ont des colonnes differentes, et cela rend l'API robuste si
le schema d'un datamart evolue cote Spark.
"""

from typing import Any, Dict, List

from pydantic import BaseModel, Field


class Token(BaseModel):
    """Reponse de l'endpoint /auth/login."""

    access_token: str
    token_type: str = "bearer"


class PaginatedResponse(BaseModel):
    """Reponse standard de tous les endpoints datamart."""

    datamart: str = Field(..., description="Nom de la table datamart interrogee")
    page: int = Field(..., description="Page courante")
    page_size: int = Field(..., description="Nombre de lignes par page")
    total_rows: int = Field(..., description="Nombre total de lignes du datamart")
    total_pages: int = Field(..., description="Nombre total de pages")
    data: List[Dict[str, Any]] = Field(..., description="Lignes de la page courante")
