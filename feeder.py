# -*- coding: utf-8 -*-
import argparse
import configparser
import logging
import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ---------------------------------------------
# LOGGING
# ---------------------------------------------

def setup_logger(log_dir):
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "feeder.txt")

    logger = logging.getLogger("feeder")
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


# ---------------------------------------------
# ARGUMENTS
# ---------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Feeder - ingestion CSV vers HDFS Parquet")
    parser.add_argument("--config", required=True, help="Chemin vers config.ini")
    return parser.parse_args()


# ---------------------------------------------
# COLONNES A CONSERVER
# ---------------------------------------------

DVF_COLUMNS = [
    "No disposition",
    "Date mutation",
    "Nature mutation",
    "Valeur fonciere",
    "Code departement",
    "Code commune",
    "Commune",
    "Type local",
    "Surface reelle bati",
    "Nombre pieces principales",
    "Surface terrain",
]

DVF_RENAME = {
    "No disposition":            "id_mutation",
    "Date mutation":             "date_mutation",
    "Nature mutation":           "nature_mutation",
    "Valeur fonciere":           "valeur_fonciere",
    "Code departement":          "code_departement",
    "Code commune":              "code_commune",
    "Commune":                   "nom_commune",
    "Type local":                "type_local",
    "Surface reelle bati":       "surface_reelle_bati",
    "Nombre pieces principales": "nombre_pieces_principales",
    "Surface terrain":           "surface_terrain",
}

COMMUNES_COLUMNS = [
    "code_insee",
    "nom_standard",
    "dep_code",
    "dep_nom",
    "reg_code",
    "reg_nom",
    "population",
]


# ---------------------------------------------
# INGESTION DVF
# ---------------------------------------------

def ingest_dvf(spark, dvf_path, raw_dvf_path, partition_date, log):
    log.info("Lecture du fichier DVF : {}".format(dvf_path))

    df = spark.read \
        .option("sep", "|") \
        .option("header", "true") \
        .option("encoding", "ISO-8859-1") \
        .option("inferSchema", "false") \
        .option("quote", "") \
        .csv(dvf_path)

    nb_lignes = df.count()
    log.info("DVF brut charge - {} lignes, {} colonnes".format(nb_lignes, len(df.columns)))

    existing_cols = [c for c in DVF_COLUMNS if c in df.columns]
    missing = [c for c in DVF_COLUMNS if c not in df.columns]
    if missing:
        log.error("Colonnes manquantes dans DVF : {}".format(missing))
        raise ValueError("Colonnes manquantes : {}".format(missing))

    df = df.select(existing_cols)

    for old_name, new_name in DVF_RENAME.items():
        df = df.withColumnRenamed(old_name, new_name)

    df = df \
        .withColumn("year",  F.lit(partition_date["year"])) \
        .withColumn("month", F.lit(partition_date["month"])) \
        .withColumn("day",   F.lit(partition_date["day"]))

    log.info("Ecriture Parquet DVF vers : {}".format(raw_dvf_path))

    df.write \
        .mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .parquet(raw_dvf_path)

    log.info("DVF ecrit avec succes en Parquet partitionne")
    return nb_lignes


# ---------------------------------------------
# INGESTION COMMUNES
# ---------------------------------------------

def ingest_communes(spark, communes_path, raw_communes_path, partition_date, log):
    log.info("Lecture du fichier communes : {}".format(communes_path))

    df = spark.read \
        .option("sep", ",") \
        .option("header", "true") \
        .option("encoding", "UTF-8") \
        .option("inferSchema", "false") \
        .csv(communes_path)

    nb_lignes = df.count()
    log.info("Communes brut charge - {} lignes, {} colonnes".format(nb_lignes, len(df.columns)))

    existing_cols = [c for c in COMMUNES_COLUMNS if c in df.columns]
    missing = [c for c in COMMUNES_COLUMNS if c not in df.columns]
    if missing:
        log.error("Colonnes manquantes dans communes : {}".format(missing))
        raise ValueError("Colonnes manquantes : {}".format(missing))

    df = df.select(existing_cols)

    df = df \
        .withColumn("year",  F.lit(partition_date["year"])) \
        .withColumn("month", F.lit(partition_date["month"])) \
        .withColumn("day",   F.lit(partition_date["day"]))

    log.info("Ecriture Parquet communes vers : {}".format(raw_communes_path))

    df.write \
        .mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .parquet(raw_communes_path)

    log.info("Communes ecrites avec succes en Parquet partitionne")
    return nb_lignes


# ---------------------------------------------
# MAIN
# ---------------------------------------------

def main():
    args = parse_args()

    config = configparser.ConfigParser()
    config.read(args.config)

    log_dir           = config["local"]["log_dir"]
    dvf_path          = config["local"]["dvf_csv_path"]
    communes_path     = config["local"]["communes_csv_path"]
    raw_dvf_path      = config["hdfs"]["raw_dvf_path"]
    raw_communes_path = config["hdfs"]["raw_communes_path"]

    log = setup_logger(log_dir)
    log.info("=== Demarrage du feeder ===")

    today = datetime.today()
    partition_date = {
        "year":  str(today.year),
        "month": str(today.month).zfill(2),
        "day":   str(today.day).zfill(2),
    }
    log.info("Partition date : year={} / month={} / day={}".format(
        partition_date["year"], partition_date["month"], partition_date["day"]
    ))

    spark = SparkSession.builder \
        .appName(config["spark"]["app_name_feeder"]) \
        .config("spark.sql.warehouse.dir", config["hdfs"]["silver_path"]) \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        nb_dvf = ingest_dvf(spark, dvf_path, raw_dvf_path, partition_date, log)
        log.info("DVF ingere : {} lignes".format(nb_dvf))

        nb_communes = ingest_communes(spark, communes_path, raw_communes_path, partition_date, log)
        log.info("Communes ingerees : {} lignes".format(nb_communes))

        log.info("=== Feeder termine avec succes ===")

    except Exception as e:
        log.error("Erreur fatale dans le feeder : {}".format(e))
        spark.stop()
        sys.exit(1)

    spark.stop()


if __name__ == "__main__":
    main()
