#!/usr/bin/env bash
# ====================================================================
#  Data Platform - Marche Immobilier Francais
#  Lance l'API REST + le Dashboard en une seule commande.
#
#  Prerequis : le cluster Docker et le container "postgres" doivent
#  deja tourner, et les datamarts doivent etre crees (datamart.py).
#
#  Usage (depuis Git Bash) :  ./start_api_dashboard.sh
# ====================================================================

# Se placer dans le dossier du script (racine du projet)
cd "$(dirname "$0")" || exit 1

echo "============================================"
echo "  Data Platform - Marche Immobilier"
echo "============================================"

echo ""
echo "[1/3] Installation des dependances Python..."
pip install -r requirements.txt

echo ""
echo "[2/3] Demarrage de l'API REST (port 8000)..."
python -m uvicorn api.app:app &
API_PID=$!

# Arreter l'API automatiquement quand on quitte le dashboard (Ctrl+C)
trap 'echo ""; echo "Arret des services..."; kill $API_PID 2>/dev/null' EXIT

echo "      Attente du demarrage de l'API..."
sleep 6

echo ""
echo "[3/3] Demarrage du Dashboard Streamlit (port 8501)..."
echo "============================================"
echo "  API       : http://localhost:8000/docs"
echo "  Dashboard : http://localhost:8501"
echo "============================================"
echo ""
echo "Ctrl+C dans cette fenetre arrete l'API ET le dashboard."
echo ""

python -m streamlit run dashboard/app.py
