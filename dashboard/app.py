# -*- coding: utf-8 -*-
"""
app.py - Dashboard Streamlit du marche immobilier francais.

Le dashboard ne touche PAS PostgreSQL directement : il consomme l'API REST
securisee (couche au-dessus des datamarts). Il demontre ainsi toute la
chaine medaillon : raw -> silver -> datamarts -> API -> visualisation.

Lancement (depuis la racine du projet) :
    streamlit run dashboard/app.py

Pre-requis : l'API doit etre lancee (uvicorn api.app:app).
"""

import configparser
import os

import pandas as pd
import plotly.express as px
import requests
import streamlit as st

# ---------------------------------------------
# CONFIGURATION (lue depuis config/config.ini)
# ---------------------------------------------

CONFIG_PATH = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "config", "config.ini")
)
_config = configparser.ConfigParser()
_config.read(CONFIG_PATH)
_api = _config["api"]

API_URL = _api.get("api_url", "http://localhost:8000")
LOGIN_USER = _api.get("login_user", "admin")
LOGIN_PASSWORD = _api.get("login_password", "admin")

st.set_page_config(
    page_title="Marche Immobilier FR",
    page_icon="🏠",
    layout="wide",
)


# ---------------------------------------------
# ACCES A L'API
# ---------------------------------------------

@st.cache_data(ttl=3000, show_spinner=False)
def get_token():
    """Authentifie le dashboard aupres de l'API et recupere un token JWT."""
    resp = requests.post(
        "{}/auth/login".format(API_URL),
        data={"username": LOGIN_USER, "password": LOGIN_PASSWORD},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


@st.cache_data(ttl=600, show_spinner="Chargement des donnees depuis l'API...")
def fetch_datamart(endpoint, page_size=1000):
    """
    Recupere l'integralite d'un datamart en parcourant toutes les pages
    de l'API (demonstration concrete de la pagination).
    """
    token = get_token()
    headers = {"Authorization": "Bearer {}".format(token)}

    rows = []
    page = 1
    while True:
        resp = requests.get(
            "{}/datamarts/{}".format(API_URL, endpoint),
            headers=headers,
            params={"page": page, "page_size": page_size},
            timeout=30,
        )
        resp.raise_for_status()
        payload = resp.json()
        rows.extend(payload["data"])
        if page >= payload["total_pages"]:
            break
        page += 1

    return pd.DataFrame(rows)


# ---------------------------------------------
# EN-TETE
# ---------------------------------------------

st.title("🏠 Marche Immobilier Francais — 2025")
st.caption(
    "Data platform medaillon · donnees DVF 2025 (data.gouv.fr) · "
    "visualisation alimentee par l'API REST"
)

# Chargement des 4 datamarts via l'API
try:
    df_commune = fetch_datamart("prix-par-commune")
    df_evolution = fetch_datamart("evolution-temporelle")
    df_densite = fetch_datamart("prix-par-densite")
    df_segment = fetch_datamart("segmentation-biens")
except requests.exceptions.RequestException as e:
    st.error(
        "Impossible de contacter l'API ({}).\n\n"
        "Verifie qu'elle est bien lancee :  uvicorn api.app:app\n\n"
        "Detail technique : {}".format(API_URL, e)
    )
    st.stop()

# ---------------------------------------------
# INDICATEURS CLES (KPI)
# ---------------------------------------------

def _fr(n):
    """Formate un nombre avec des espaces comme separateurs de milliers."""
    return "{:,.0f}".format(n).replace(",", " ")


c1, c2, c3 = st.columns(3)
c1.metric("Transactions analysees", _fr(df_commune["nb_transactions"].sum()))
c2.metric("Communes couvertes", _fr(df_commune["code_commune"].nunique()))
c3.metric("Prix moyen national au m²", _fr(df_commune["prix_moyen_m2"].mean()) + " €")

st.divider()

# ---------------------------------------------
# GRAPHIQUE 1 : evolution temporelle (line chart)
# ---------------------------------------------

st.subheader("1 · Evolution mensuelle du prix moyen au m²")
df_e = df_evolution.sort_values(["annee", "mois"])
fig1 = px.line(
    df_e,
    x="annee_mois",
    y="prix_moyen_m2",
    color="type_local",
    markers=True,
    labels={
        "annee_mois": "Mois",
        "prix_moyen_m2": "Prix moyen au m² (€)",
        "type_local": "Type de bien",
    },
)
fig1.update_layout(legend_title_text="Type de bien", hovermode="x unified")
st.plotly_chart(fig1, use_container_width=True)

# ---------------------------------------------
# GRAPHIQUE 2 : prix par densite (bar chart groupe)
# ---------------------------------------------

st.subheader("2 · Prix moyen au m² par taille de commune")
df_d = df_densite.sort_values("ordre_tranche")
fig2 = px.bar(
    df_d,
    x="tranche_population",
    y="prix_moyen_m2",
    color="type_local",
    barmode="group",
    labels={
        "tranche_population": "Tranche de population",
        "prix_moyen_m2": "Prix moyen au m² (€)",
        "type_local": "Type de bien",
    },
)
fig2.update_layout(legend_title_text="Type de bien")
st.plotly_chart(fig2, use_container_width=True)

# ---------------------------------------------
# GRAPHIQUE 3 : segmentation par surface (bar chart groupe)
# ---------------------------------------------

st.subheader("3 · Valeur fonciere moyenne par tranche de surface")
ordre_surface = [
    "Tres petit (< 30 m2)",
    "Petit (30 - 60 m2)",
    "Moyen (60 - 100 m2)",
    "Grand (100 - 150 m2)",
    "Tres grand (> 150 m2)",
]
seg = (
    df_segment.groupby(["tranche_surface", "type_local"], as_index=False)[
        "valeur_fonciere_moyenne"
    ]
    .mean()
)
seg["tranche_surface"] = pd.Categorical(
    seg["tranche_surface"], categories=ordre_surface, ordered=True
)
seg = seg.sort_values("tranche_surface")
fig3 = px.bar(
    seg,
    x="tranche_surface",
    y="valeur_fonciere_moyenne",
    color="type_local",
    barmode="group",
    labels={
        "tranche_surface": "Tranche de surface",
        "valeur_fonciere_moyenne": "Valeur fonciere moyenne (€)",
        "type_local": "Type de bien",
    },
)
fig3.update_layout(legend_title_text="Type de bien")
st.plotly_chart(fig3, use_container_width=True)

# ---------------------------------------------
# GRAPHIQUE 4 : top 15 communes les plus cheres
# ---------------------------------------------

st.subheader("4 · Top 15 des communes les plus cheres")
type_choisi = st.radio(
    "Type de bien :",
    options=["Appartement", "Maison"],
    horizontal=True,
)
top = (
    df_commune[df_commune["type_local"] == type_choisi]
    .nlargest(15, "prix_median_m2")
    .sort_values("prix_median_m2")
)
fig4 = px.bar(
    top,
    x="prix_median_m2",
    y="nom_standard",
    orientation="h",
    color="prix_median_m2",
    color_continuous_scale="Reds",
    labels={
        "prix_median_m2": "Prix median au m² (€)",
        "nom_standard": "Commune",
    },
)
fig4.update_layout(coloraxis_showscale=False)
st.plotly_chart(fig4, use_container_width=True)

# ---------------------------------------------
# DONNEES BRUTES (optionnel)
# ---------------------------------------------

with st.expander("Voir les donnees brutes des datamarts"):
    onglet = st.selectbox(
        "Datamart :",
        ["prix_par_commune", "evolution_temporelle", "prix_par_densite", "segmentation_biens"],
    )
    tables = {
        "prix_par_commune": df_commune,
        "evolution_temporelle": df_evolution,
        "prix_par_densite": df_densite,
        "segmentation_biens": df_segment,
    }
    st.dataframe(tables[onglet], use_container_width=True)
