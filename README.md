# Projet Big Data Framework

> Analyse du Marché Immobilier Français en 2025 -
> Auteurs : **Adam GHORIFA · Zain MEHR**

---

## 📋 Contexte et problématique métier

A REMPLIR

---

## 📁 Structure du projet


```
projet/
│
├── config/
│   └── config.ini                  # chemins HDFS
│
├── data/
│   ├── dvf_2025.txt               # source brute DVF
│   └── communes_2025.csv           # source brute communes
│
├── logs/
│   ├── feeder.txt
│   ├── processor.txt
│   └── datamart.txt
│
├── feeder.py                       # CSV + txt → Parquet /raw HDFS
├── processor.py                    # /raw → Hive silver
├── datamart.py                     # silver → PostgreSQL
│
├── api/
│   ├── app.py                      # FastAPI + JWT
│   ├── auth.py                     # génération / vérification JWT
│   ├── models.py                   # schémas Pydantic
│   └── database.py                 # connexion PostgreSQL
│
├── dashboard/
│   └── app.py                      # Streamlit + Plotly
│
├── requirements.txt                # dépendances Python
├── docker-hadoop-spark-master
│   └── ...
└── README.md
```

---

## Étape 1 — Copier les fichiers dans le namenode

```bash
docker cp data/dvf_2025.txt namenode:dvf_2025.txt
docker cp data/communes_2025.csv namenode:communes_2025.csv
```

## Étape 2 — Créer les répertoires HDFS

```bash
docker exec -it namenode bash

# Créer les répertoires raw
hdfs dfs -mkdir -p /raw/dvf
hdfs dfs -mkdir -p /raw/communes

# Créer le répertoire silver
hdfs dfs -mkdir -p /silver

# Vérifier
hdfs dfs -ls /
```

## Étape 3 — Copier les fichiers sources dans le container Spark

```bash
docker cp data/dvf_2025.txt spark-master:/dvf_2025.txt
docker cp data/communes_2025.csv spark-master:/communes_2025.csv
```

## Étape 4 — Copier feeder.py dans le container Spark

```bash
docker cp feeder.py spark-master:feeder.py
docker cp config/config.ini spark-master:config.ini
```


## Étape 5 — Exécuter le feeder depuis le container Spark

```bash
docker exec -it spark-master bash

# On exécute la commande qui lance le feeder
PYSPARK_PYTHON=python3 spark/bin/spark-submit   --master local[*]   --name feeder   feeder.py   --config config.ini

# Exemple d'affiche en cas de succès de l'exécution
[2026-05-18 10:04:55] [INFO] DVF ecrit avec succes en Parquet partitionne
[2026-05-18 10:04:55] [INFO] DVF ingere : 3714829 lignes
[2026-05-18 10:04:55] [INFO] Lecture du fichier communes : /communes_2025.csv
[2026-05-18 10:04:56] [INFO] Communes brut charge - 34935 lignes, 47 colonnes
[2026-05-18 10:04:57] [INFO] Ecriture Parquet communes vers : hdfs://namenode:9000/raw/communes
[2026-05-18 10:04:59] [INFO] Communes ecrites avec succes en Parquet partitionne
[2026-05-18 10:04:59] [INFO] Communes ingerees : 34935 lignes
[2026-05-18 10:04:59] [INFO] === Feeder termine avec succes ===
```

## Étape 6 — Copier processor.py dans le container Spark

```bash
docker cp processor.py spark-master:/processor.py
```

## Étape 7 — Exécuter le feeder depuis le container Spark

```bash
docker exec -it spark-master bash

# On exécute la commande qui lance le feeder
PYSPARK_PYTHON=python3 spark/bin/spark-submit \
  --master local[*] \
  --name processor \
  processor.py \
  --config config.ini

# Exemple d'affiche en cas de succès de l'exécution
"[2026-05-20 11:41:02] [INFO] Regle 2 (surface_reelle_bati > 0) - 1230819 lignes restantes
[2026-05-20 11:41:03] [INFO] Regle 3 (type_local Maison/Appartement) - 1122389 lignes restantes
[2026-05-20 11:41:04] [INFO] Regle 4 (code_insee 5 caracteres) - 1108781 lignes restantes
[2026-05-20 11:41:05] [INFO] Regle 5 (date_mutation non nulle) - 1108781 lignes restantes
[2026-05-20 11:41:06] [INFO] Validation terminee - 2606048 lignes supprimees sur 3714829
[2026-05-20 11:41:06] [INFO] Debut des transformations
[2026-05-20 11:41:06] [INFO] Transformations terminees - prix_m2 calcule
[2026-05-20 11:41:06] [INFO] Jointure DVF x communes sur code_insee_reconstruit / code_insee
[2026-05-20 11:41:10] [INFO] Jointure terminee - 1020868 lignes
[2026-05-20 11:41:10] [INFO] Application du cache avant les window functions
[2026-05-20 11:41:20] [INFO] Cache applique
[2026-05-20 11:41:20] [INFO] Window 1 (prix median par commune) appliquee
[2026-05-20 11:41:20] [INFO] Window 2 (rank par type local) appliquee
[2026-05-20 11:41:20] [INFO] Window 3 (lag evolution temporelle) appliquee
[2026-05-20 11:41:20] [INFO] Toutes les window functions appliquees
[2026-05-20 11:41:20] [INFO] Creation de la base Hive si elle n'existe pas : silver
[2026-05-20 11:42:18] [INFO] Table Hive silver.dvf_enrichi ecrite avec succes
[2026-05-20 11:42:18] [INFO] === Processor termine avec succes ==="
```