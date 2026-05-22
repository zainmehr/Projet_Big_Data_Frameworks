# -*- coding: utf-8 -*-
import argparse
import configparser
import logging
import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType, IntegerType


# ---------------------------------------------
# LOGGING
# ---------------------------------------------

def setup_logger(log_dir):
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "processor.txt")

    logger = logging.getLogger("processor")
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
    parser = argparse.ArgumentParser(description="Processor - nettoyage et enrichissement vers silver")
    parser.add_argument("--config", required=True, help="Chemin vers config.ini")
    return parser.parse_args()


# ---------------------------------------------
# LECTURE RAW
# ---------------------------------------------

def read_raw(spark, raw_dvf_path, raw_communes_path, log):
    log.info("Lecture DVF depuis : {}".format(raw_dvf_path))
    dvf = spark.read.parquet(raw_dvf_path)
    log.info("DVF raw charge - {} lignes".format(dvf.count()))

    log.info("Lecture communes depuis : {}".format(raw_communes_path))
    communes = spark.read.parquet(raw_communes_path)
    log.info("Communes raw chargees - {} lignes".format(communes.count()))

    return dvf, communes


# ---------------------------------------------
# RECONSTRUCTION DU CODE INSEE
# Dans le DVF : code_departement="01" + code_commune="158"
# Dans communes : code_insee="01158"
# On reconstruit : code_departement + code_commune.zfill(3)
# ---------------------------------------------

def reconstruct_code_insee(dvf, log):
    log.info("Reconstruction du code INSEE : code_departement + code_commune (zero-padded a 3)")

    dvf = dvf.withColumn(
        "code_insee_reconstruit",
        F.concat(
            F.col("code_departement"),
            F.lpad(F.col("code_commune"), 3, "0")
        )
    )

    log.info("Code INSEE reconstruit - exemple : 01 + 158 = 01158")
    return dvf


# ---------------------------------------------
# VALIDATION - 5 REGLES
# ---------------------------------------------

def validate(dvf, log):
    nb_initial = dvf.count()
    log.info("Validation - nb lignes initial : {}".format(nb_initial))

    # Regle 1 : valeur_fonciere non nulle
    dvf = dvf.filter(F.col("valeur_fonciere").isNotNull())
    log.info("Regle 1 (valeur_fonciere non nulle) - {} lignes restantes".format(dvf.count()))

    # Regle 2 : surface_reelle_bati non nulle et > 0
    dvf = dvf.filter(
        F.col("surface_reelle_bati").isNotNull() &
        (F.col("surface_reelle_bati") != "0") &
        (F.col("surface_reelle_bati") != "")
    )
    log.info("Regle 2 (surface_reelle_bati > 0) - {} lignes restantes".format(dvf.count()))

    # Regle 3 : type_local uniquement Maison ou Appartement
    dvf = dvf.filter(F.col("type_local").isin("Maison", "Appartement"))
    log.info("Regle 3 (type_local Maison/Appartement) - {} lignes restantes".format(dvf.count()))

    # Regle 4 : code_insee_reconstruit non nul et 5 caracteres
    dvf = dvf.filter(
        F.col("code_insee_reconstruit").isNotNull() &
        (F.length(F.col("code_insee_reconstruit")) == 5)
    )
    log.info("Regle 4 (code_insee 5 caracteres) - {} lignes restantes".format(dvf.count()))

    # Regle 5 : date_mutation non nulle
    dvf = dvf.filter(F.col("date_mutation").isNotNull())
    log.info("Regle 5 (date_mutation non nulle) - {} lignes restantes".format(dvf.count()))

    nb_final = dvf.count()
    log.info("Validation terminee - {} lignes supprimees sur {}".format(
        nb_initial - nb_final, nb_initial
    ))

    return dvf


# ---------------------------------------------
# TRANSFORMATIONS
# ---------------------------------------------

def transform(dvf, log):
    log.info("Debut des transformations")

    # Correction virgule decimale sur valeur_fonciere (ex: 468000,00 -> 468000.00)
    dvf = dvf.withColumn(
        "valeur_fonciere",
        F.regexp_replace(F.col("valeur_fonciere"), ",", ".").cast(FloatType())
    )

    # Cast surface_reelle_bati en Float
    dvf = dvf.withColumn(
        "surface_reelle_bati",
        F.col("surface_reelle_bati").cast(FloatType())
    )

    # Cast nombre_pieces_principales en Integer
    dvf = dvf.withColumn(
        "nombre_pieces_principales",
        F.col("nombre_pieces_principales").cast(IntegerType())
    )

    # Cast surface_terrain en Float
    dvf = dvf.withColumn(
        "surface_terrain",
        F.col("surface_terrain").cast(FloatType())
    )

    # Conversion date_mutation en type Date (format dd/MM/yyyy)
    dvf = dvf.withColumn(
        "date_mutation",
        F.to_date(F.col("date_mutation"), "dd/MM/yyyy")
    )

    # Extraction annee et mois pour les datamarts temporels
    dvf = dvf.withColumn("annee", F.year(F.col("date_mutation")))
    dvf = dvf.withColumn("mois",  F.month(F.col("date_mutation")))

    # Calcul prix_m2
    dvf = dvf.withColumn(
        "prix_m2",
        F.round(F.col("valeur_fonciere") / F.col("surface_reelle_bati"), 2)
    )

    # Filtrer les prix_m2 aberrants (< 100 ou > 100000)
    dvf = dvf.filter(
        (F.col("prix_m2") >= 100) & (F.col("prix_m2") <= 100000)
    )

    log.info("Transformations terminees - prix_m2 calcule")
    return dvf


# ---------------------------------------------
# JOINTURE DVF x COMMUNES
# ---------------------------------------------

def join_communes(dvf, communes, log):
    log.info("Jointure DVF x communes sur code_insee_reconstruit / code_insee")

    # Cast population en Integer
    communes = communes.withColumn(
        "population",
        F.col("population").cast(IntegerType())
    )

    communes_slim = communes.select(
        "code_insee",
        "nom_standard",
        "dep_code",
        "dep_nom",
        "reg_code",
        "reg_nom",
        "population"
    )

    df_joined = dvf.join(
        communes_slim,
        dvf["code_insee_reconstruit"] == communes_slim["code_insee"],
        how="inner"
    ).drop("code_insee")

    nb = df_joined.count()
    log.info("Jointure terminee - {} lignes".format(nb))

    return df_joined


# ---------------------------------------------
# CACHE + WINDOW FUNCTIONS
# ---------------------------------------------

def apply_window_functions(df, log):
    log.info("Application du cache avant les window functions")

    # Cache apres la jointure, avant les agregations - visible dans Spark UI
    df.cache()
    df.count()  # materialisation du cache
    log.info("Cache applique")

    # Window 1 : prix median par commune et type de bien
    # utilisation de expr() car percentile_approx n'est pas dans F sur Spark 3.0
    median_df = df.groupBy("code_insee_reconstruit", "type_local") \
        .agg(
            F.expr("percentile_approx(prix_m2, 0.5)").alias("prix_m2_median_commune")
        )

    df = df.join(
        median_df,
        on=["code_insee_reconstruit", "type_local"],
        how="left"
    )

    log.info("Window 1 (prix median par commune) appliquee")

    # Window 2 : rang du prix_m2 par type de bien
    window_type = Window.partitionBy("type_local").orderBy(F.col("prix_m2").desc())
    df = df.withColumn(
        "rang_prix_type_local",
        F.rank().over(window_type)
    )
    log.info("Window 2 (rank par type local) appliquee")

    # Window 3 : LAG pour evolution temporelle par commune et type
    window_temps = Window.partitionBy("code_insee_reconstruit", "type_local").orderBy("date_mutation")
    df = df.withColumn(
        "prix_m2_precedent",
        F.lag(F.col("prix_m2"), 1).over(window_temps)
    )
    log.info("Window 3 (lag evolution temporelle) appliquee")

    log.info("Toutes les window functions appliquees")
    return df


# ---------------------------------------------
# ECRITURE HIVE SILVER
# ---------------------------------------------

def write_silver(spark, df, silver_path, hive_database, hive_table, partition_date, log):
    log.info("Creation de la base Hive si elle n'existe pas : {}".format(hive_database))
    spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(hive_database))

    # Ajout des colonnes de partition silver
    df = df \
        .withColumn("year",  F.lit(partition_date["year"])) \
        .withColumn("month", F.lit(partition_date["month"])) \
        .withColumn("day",   F.lit(partition_date["day"]))

    log.info("Ecriture silver vers Hive : {}.{}".format(hive_database, hive_table))

    df.write \
        .mode("overwrite") \
        .format("parquet") \
        .partitionBy("year", "month", "day") \
        .saveAsTable("{}.{}".format(hive_database, hive_table))

    log.info("Table Hive {}.{} ecrite avec succes".format(hive_database, hive_table))


# ---------------------------------------------
# MAIN
# ---------------------------------------------

def main():
    args = parse_args()

    config = configparser.ConfigParser()
    config.read(args.config)

    log_dir           = config["local"]["log_dir"]
    raw_dvf_path      = config["hdfs"]["raw_dvf_path"]
    raw_communes_path = config["hdfs"]["raw_communes_path"]
    silver_path       = config["hdfs"]["silver_path"]
    hive_database     = config["hive"]["database"]
    hive_table        = config["hive"]["table_dvf_enrichi"]

    log = setup_logger(log_dir)
    log.info("=== Demarrage du processor ===")

    today = datetime.today()
    partition_date = {
        "year":  str(today.year),
        "month": str(today.month).zfill(2),
        "day":   str(today.day).zfill(2),
    }

    spark = SparkSession.builder \
        .appName(config["spark"]["app_name_processor"]) \
        .config("spark.sql.warehouse.dir", silver_path) \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        # Lecture
        dvf, communes = read_raw(spark, raw_dvf_path, raw_communes_path, log)

        # Reconstruction code INSEE avant validation
        dvf = reconstruct_code_insee(dvf, log)

        # Validation
        dvf = validate(dvf, log)

        # Transformations
        dvf = transform(dvf, log)

        # Jointure
        df = join_communes(dvf, communes, log)

        # Cache + window functions
        df = apply_window_functions(df, log)

        # Ecriture Hive silver
        write_silver(spark, df, silver_path, hive_database, hive_table, partition_date, log)

        log.info("=== Processor termine avec succes ===")

    except Exception as e:
        log.error("Erreur fatale dans le processor : {}".format(e))
        spark.stop()
        sys.exit(1)

    spark.stop()


if __name__ == "__main__":
    main()