@echo off
REM ====================================================================
REM  Data Platform - Marche Immobilier Francais
REM  Lance l'API REST + le Dashboard en une seule commande.
REM
REM  Prerequis : le cluster Docker et le container "postgres" doivent
REM  deja tourner, et les datamarts doivent etre crees (datamart.py).
REM ====================================================================

cd /d "%~dp0"

echo ============================================
echo   Data Platform - Marche Immobilier
echo ============================================
echo.

echo [1/3] Installation des dependances Python...
pip install -r requirements.txt
echo.

echo [2/3] Demarrage de l'API REST...
start "API - Marche Immobilier" cmd /k python -m uvicorn api.app:app
echo       Attente du demarrage de l'API...
timeout /t 6 /nobreak >nul
echo.

echo [3/3] Demarrage du Dashboard Streamlit...
start "Dashboard - Marche Immobilier" cmd /k python -m streamlit run dashboard/app.py
echo.

echo ============================================
echo   API       : http://localhost:8000/docs
echo   Dashboard : http://localhost:8501
echo ============================================
echo.
echo Deux fenetres se sont ouvertes (API et Dashboard).
echo Pour arreter les services, fermez ces deux fenetres.
echo.
pause
