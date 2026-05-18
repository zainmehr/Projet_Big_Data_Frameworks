# Projet Big Data Framework

> Analyse du Marché Immobilier Français en 2025 
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

## Étape 3 — Copier les CSV dans HDFS

```bash
# Toujours depuis namenode
hdfs dfs -put dvf_2025.txt /raw/dvf/
hdfs dfs -put communes_2025.csv /raw/communes/

# Vérifier
hdfs dfs -ls /raw/dvf/
hdfs dfs -ls /raw/communes/
```

## Étape 4 — Copier feeder.py dans le container Spark

```bash
docker cp feeder.py spark-master:feeder.py
docker cp config/config.ini spark-master:config.ini
```

