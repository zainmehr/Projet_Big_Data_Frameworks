# Projet Big Data Framework

> Analyse du Marché Immobilier Français en 2025
> Auteurs : **Adam GHORIFA · Zain MEHR**

---

## Contexte et problématique métier

Le marché immobilier français représente un volume de transactions considérable chaque année. Ce projet construit une data platform permettant d'analyser les mutations foncières sur l'ensemble du territoire en 2025 : évolution des prix au m², disparités géographiques entre communes, types de biens les plus échangés et volumes de transactions par région. Elle permet également de comparer les prix au m² selon la population d'une commune.

**Problématique :** Comment évoluent les prix de l'immobilier selon les territoires, les types de biens et les dynamiques locales ?

---

## Architecture (médaillon)

```
Source DVF 2025 ──► feeder.py ──► /raw (HDFS, Parquet)
                                      │
                                      ▼
                                 processor.py ──► /silver (Hive, Parquet)
                                                      │
                                                      ▼
                                                 datamart.py ──► PostgreSQL (4 datamarts)
                                                                       │
                                                          ┌────────────┴────────────┐
                                                          ▼                         ▼
                                                    API REST (FastAPI)        Dashboard (Streamlit)
```

---

## Stack technique

| Composant | Technologie |
|---|---|
| Stockage distribué | HDFS (Hadoop) |
| Format de fichier | Apache Parquet |
| Catalogue de métadonnées | Apache Hive |
| Moteur de traitement | Apache Spark 3.0 (PySpark) |
| Base relationnelle | PostgreSQL 14 |
| API REST | FastAPI + JWT |
| Visualisation | Streamlit + Plotly |
| Conteneurisation | Docker |

---

## Structure du projet

```
Projet_Big_Data_Frameworks/
│
├── config/
│   └── config.ini                  # chemins HDFS, BDD, ports, paramètres API
│
├── data/
│   ├── dvf_2025.zip                # source brute DVF compressée (à décompresser)
│   └── communes_2025.csv           # source brute communes
│
├── logs/
│   ├── feeder.txt
│   ├── processor.txt
│   └── datamart.txt
│
├── feeder.py                       # txt/CSV -> Parquet /raw HDFS
├── processor.py                    # /raw -> validation/jointure/window -> Hive silver
├── datamart.py                     # silver -> 4 datamarts PostgreSQL
│
├── api/
│   ├── app.py                      # endpoints FastAPI + pagination
│   ├── auth.py                     # génération / vérification JWT
│   ├── models.py                   # schémas Pydantic
│   └── database.py                 # connexion PostgreSQL
│
├── dashboard/
│   └── app.py                      # Streamlit + Plotly (4 graphiques)
│
├── postgresql-42.6.0.jar           # driver JDBC PostgreSQL (pour datamart.py)
├── requirements.txt                # dépendances Python (API + dashboard)
└── README.md
```

---

## Prérequis

- **Docker Desktop** installé et démarré (allouer idéalement 6 à 8 Go de RAM dans les réglages).
- **Python 3.x** installé sur la machine hôte.
- Le **cluster Docker Hadoop + Spark** (dossier `docker-hadoop-spark/`). Ce dossier est volumineux et n'est **pas inclus dans le dépôt** ; il est fourni séparément. Le placer à côté du projet.

---

# Lancement — Partie 1 : Pipeline de données

### Étape 0 — Démarrer le cluster Hadoop + Spark

Depuis le dossier du cluster :

```bash
cd docker-hadoop-spark
docker compose up -d
docker ps
```

`docker ps` doit afficher au minimum les containers `namenode` et `spark-master` au statut `Up`. Revenir ensuite dans le dossier du projet.

---

### Étape 1 — Créer le container PostgreSQL

> **Nom du réseau :** Docker nomme le réseau du cluster d'après le dossier (`<nom_du_dossier>_default`). Vérifier le nom exact avec `docker network ls` et l'utiliser dans `--network` ci-dessous. Ici le dossier s'appelle `docker-hadoop-spark`, donc le réseau est `docker-hadoop-spark_default`.

```bash
docker run -d \
  --name postgres \
  --network docker-hadoop-spark_default \
  -e POSTGRES_DB=immo_datamarts \
  -e POSTGRES_USER=immo_user \
  -e POSTGRES_PASSWORD=immo_pass \
  -p 5433:5432 \
  postgres:14
```

> Le port hôte **5433** est utilisé (le 5432 étant déjà pris par le metastore Hive). C'est ce port que l'API utilise (`localhost:5433`).

Vérifier : `docker ps --filter name=postgres` → statut `Up`.

---

### Étape 2 — Créer les répertoires HDFS

```bash
docker exec -it namenode bash

hdfs dfs -mkdir -p /raw/dvf /raw/communes /silver
hdfs dfs -ls /

exit
```

---

### Étape 3 — Préparer le fichier DVF

Le dépôt fournit `data/dvf_2025.zip`. Le décompresser et le renommer en `dvf_2025.txt` :

```bash
cd data
unzip dvf_2025.zip
mv ValeursFoncieres-2025.txt dvf_2025.txt
cd ..
```

Le dossier `data/` doit alors contenir `dvf_2025.txt` (~474 Mo) et `communes_2025.csv`.

---

### Étape 4 — Copier sources, scripts et driver dans spark-master

```bash
docker cp data/dvf_2025.txt        spark-master:/dvf_2025.txt
docker cp data/communes_2025.csv   spark-master:/communes_2025.csv
docker cp feeder.py                spark-master:/feeder.py
docker cp processor.py             spark-master:/processor.py
docker cp datamart.py              spark-master:/datamart.py
docker cp config/config.ini        spark-master:/config.ini
docker cp postgresql-42.6.0.jar    spark-master:/postgresql-42.6.0.jar
```

> Le driver JDBC `postgresql-42.6.0.jar` est fourni dans le dépôt — inutile de le télécharger.

---

### Étape 5 — Exécuter le feeder

```bash
docker exec -it spark-master bash

PYSPARK_PYTHON=python3 spark/bin/spark-submit \
  --master local[*] \
  --name feeder \
  feeder.py \
  --config config.ini
```

Fin attendue :

```
[INFO] DVF ingere : 3714829 lignes
[INFO] Communes ingerees : 34935 lignes
[INFO] === Feeder termine avec succes ===
```

---

### Étape 6 — Exécuter le processor

Toujours dans le container `spark-master` :

```bash
PYSPARK_PYTHON=python3 spark/bin/spark-submit \
  --master local[*] \
  --name processor \
  processor.py \
  --config config.ini
```

C'est l'étape la plus longue (validation des 5 règles, jointure, window functions, écriture Hive). Fin attendue :

```
[INFO] Validation terminee - 2606048 lignes supprimees sur 3714829
[INFO] Jointure terminee - 1020868 lignes
[INFO] Toutes les window functions appliquees
[INFO] Table Hive silver.dvf_enrichi ecrite avec succes
[INFO] === Processor termine avec succes ===
```

---

### Étape 7 — Exécuter le datamart

Toujours dans `spark-master` (le driver JDBC est passé via `--jars`) :

```bash
PYSPARK_PYTHON=python3 spark/bin/spark-submit \
  --master local[*] \
  --name datamart \
  --jars /postgresql-42.6.0.jar \
  datamart.py \
  --config config.ini
```

Fin attendue :

```
[INFO] Datamart dm_prix_par_commune ecrit avec succes - 40176 lignes
[INFO] Datamart dm_evolution_temporelle ecrit avec succes - 24 lignes
[INFO] Datamart dm_prix_par_densite ecrit avec succes - 10 lignes
[INFO] Datamart dm_segmentation_biens ecrit avec succes - 188 lignes
[INFO] === Datamart termine avec succes ===
```

Sortir du container puis vérifier les 4 tables dans PostgreSQL :

```bash
exit
docker exec -it postgres psql -U immo_user -d immo_datamarts -c "\dt"
```

Résultat attendu :

```
                  List of relations
 Schema |          Name           | Type  |   Owner
--------+-------------------------+-------+-----------
 public | dm_evolution_temporelle | table | immo_user
 public | dm_prix_par_commune     | table | immo_user
 public | dm_prix_par_densite     | table | immo_user
 public | dm_segmentation_biens   | table | immo_user
(4 rows)
```

---

# Lancement — Partie 2 : API REST + Dashboard

L'API et le dashboard tournent **sur la machine hôte** (pas dans Docker). Le container `postgres` doit être démarré (étape 1) ; l'API s'y connecte via `localhost:5433`.

> **Raccourci :** une fois le cluster Docker démarré, le container `postgres` actif et les datamarts créés (étapes 0 à 7), il suffit d'exécuter le script `start_api_dashboard.bat` (Windows) ou `start_api_dashboard.sh` (Git Bash / Linux / Mac) pour lancer automatiquement l'API et le dashboard. Détails dans la section **Bonus — Lancement rapide** en fin de document. Sinon, suivre les étapes 8 à 10 ci-dessous.

### Étape 8 — Installer les dépendances Python

À faire une seule fois, depuis la racine du projet :

```bash
pip install -r requirements.txt
```

---

### Étape 9 — Lancer l'API REST

Depuis la **racine du projet** (important pour la résolution des imports) :

```bash
python -m uvicorn api.app:app --reload
```

> Si la commande `uvicorn` seule renvoie « command not found » (PATH Windows), utiliser bien la forme `python -m uvicorn` ci-dessus.

Logs en cas de succès :

```
INFO:     Uvicorn running on http://0.0.0.0:8000
INFO:     Application startup complete.
```

**Tester l'API** — ouvrir http://localhost:8000/docs (documentation Swagger) :

1. Cliquer sur **Authorize**, saisir les identifiants `admin` / `admin`.
2. Déplier un endpoint `GET /datamarts/...`, cliquer **Try it out** puis **Execute**.
3. La réponse `200` contient `total_rows`, `total_pages` et le tableau `data`.

---

### Étape 10 — Lancer le dashboard

Laisser l'API tournée. Dans un **second terminal**, depuis la racine du projet :

```bash
python -m streamlit run dashboard/app.py
```

Le dashboard s'ouvre sur http://localhost:8501. Il s'authentifie auprès de l'API, récupère les 4 datamarts via les endpoints paginés et affiche 3 indicateurs clés + 4 graphiques Plotly.

---

## API REST — endpoints

L'API est sécurisée par JWT : tous les endpoints `/datamarts/*` exigent un token obtenu via `/auth/login`.

| Méthode | Endpoint | Sécurité | Description |
|---|---|---|---|
| GET | `/` | Public | Santé de l'API |
| POST | `/auth/login` | Public | Retourne un token JWT (identifiants `admin` / `admin`) |
| GET | `/datamarts/prix-par-commune` | JWT | Prix moyen/médian au m² par commune |
| GET | `/datamarts/evolution-temporelle` | JWT | Évolution mensuelle des prix et volumes |
| GET | `/datamarts/prix-par-densite` | JWT | Prix par tranche de population |
| GET | `/datamarts/segmentation-biens` | JWT | Segmentation des biens par surface et pièces |

**Pagination** : chaque endpoint datamart accepte `page` (défaut 1) et `page_size` (défaut 50, max 1000).
Exemple : `GET /datamarts/prix-par-commune?page=2&page_size=100`

---

## Dashboard — visualisations

Le dashboard Streamlit consomme exclusivement l'API REST (il ne touche pas PostgreSQL directement), démontrant la chaîne complète. Il affiche :

1. **Évolution mensuelle du prix moyen au m²** — courbe par type de bien.
2. **Prix moyen au m² par taille de commune** — barres groupées maison / appartement.
3. **Valeur foncière moyenne par tranche de surface** — barres groupées.
4. **Top 15 des communes les plus chères** — barres horizontales (maison ou appartement au choix).

---

## Datamarts

| Datamart | Lignes | Description |
|---|---|---|
| dm_prix_par_commune | 40 176 | Prix moyen/médian au m² par commune et type de bien |
| dm_evolution_temporelle | 24 | Évolution mensuelle des prix et volumes de ventes |
| dm_prix_par_densite | 10 | Prix par tranche de population + tension du marché |
| dm_segmentation_biens | 188 | Segmentation par surface, pièces et type de bien |

---

## Paramétrage

Tous les paramètres (chemins HDFS, connexion PostgreSQL, ports, secret JWT, identifiants API) sont centralisés dans `config/config.ini` — aucun chemin n'est codé en dur dans les scripts. La section `[api]` contient la connexion BDD utilisée par l'API (`localhost:5433`) et les identifiants de connexion. Le `secret_key` doit être modifié pour un usage réel.

---

## Bonus — Lancement rapide (API + Dashboard)

Une fois le pipeline de données terminé (étapes 0 à 7) — c'est-à-dire le **cluster Docker démarré**, le **container `postgres` actif** et les **4 datamarts créés** — il n'est pas nécessaire de lancer manuellement les étapes 8, 9 et 10.

Deux scripts à la racine du projet exécutent tout automatiquement (installation des dépendances, démarrage de l'API puis du dashboard) :

| Système | Script | Utilisation |
|---|---|---|
| Windows | `start_api_dashboard.bat` | Double-cliquer dessus (ou l'exécuter en ligne de commande). Deux fenêtres s'ouvrent : une pour l'API, une pour le dashboard. |
| Git Bash / Linux / Mac | `start_api_dashboard.sh` | Exécuter `./start_api_dashboard.sh`. L'API tourne en arrière-plan, le dashboard au premier plan ; `Ctrl+C` arrête proprement les deux services. |

Dans les deux cas, à la fin :

- API REST : http://localhost:8000/docs
- Dashboard : http://localhost:8501

> Ces scripts remplacent uniquement les étapes 8 à 10. Le pipeline de données (étapes 0 à 7) doit être exécuté au préalable, sinon l'API n'aura aucune donnée à servir.
