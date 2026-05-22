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


# ---------------------------------------------
# LOGGING
# ---------------------------------------------

def setup_logger(log_dir):
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "datamart.txt")

    logger = logging.getLogger("datamart")
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
    parser = argparse.ArgumentParser(description="Datamart - creation des datamarts PostgreSQL")
    parser.add_argument("--config", required=True, help="Chemin vers config.ini")
    return parser.parse_args()


# ---------------------------------------------
# ECRITURE POSTGRESQL
# ---------------------------------------------

def write_to_postgres(df, table_name, jdbc_url, jdbc_driver, user, password, log):
    log.info("Ecriture du datamart : {}".format(table_name))

    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    log.info("Datamart {} ecrit avec succes - {} lignes".format(table_name, df.count()))


# ---------------------------------------------
# DM 1 : dm_prix_par_commune
# ---------------------------------------------

def build_dm_prix_par_commune(df, log):
    log.info("Construction de dm_prix_par_commune")

    dm = df.groupBy("code_insee_reconstruit", "nom_standard", "dep_nom", "reg_nom", "type_local") \
        .agg(
            F.round(F.avg("prix_m2"), 2).alias("prix_moyen_m2"),
            F.round(F.expr("percentile_approx(prix_m2, 0.5)"), 2).alias("prix_median_m2"),
            F.count("*").alias("nb_transactions"),
            F.round(F.avg("surface_reelle_bati"), 2).alias("surface_moyenne")
        ) \
        .withColumnRenamed("code_insee_reconstruit", "code_commune")

    log.info("dm_prix_par_commune construit - {} lignes".format(dm.count()))
    return dm


# ---------------------------------------------
# DM 2 : dm_evolution_temporelle
# ---------------------------------------------

def build_dm_evolution_temporelle(df, log):
    log.info("Construction de dm_evolution_temporelle")

    # Agregation mensuelle
    dm = df.groupBy("annee", "mois", "type_local") \
        .agg(
            F.round(F.avg("prix_m2"), 2).alias("prix_moyen_m2"),
            F.count("*").alias("nb_ventes")
        )

    # Colonne annee_mois pour le tri et la cle
    dm = dm.withColumn(
        "annee_mois",
        F.concat(F.col("annee").cast("string"), F.lit("-"), F.lpad(F.col("mois").cast("string"), 2, "0"))
    )

    # Window LAG pour variation mensuelle
    window_lag = Window.partitionBy("type_local").orderBy("annee", "mois")
    dm = dm.withColumn(
        "prix_moyen_m2_precedent",
        F.lag(F.col("prix_moyen_m2"), 1).over(window_lag)
    )

    dm = dm.withColumn(
        "variation_pct_mois",
        F.when(
            F.col("prix_moyen_m2_precedent").isNotNull(),
            F.round(
                ((F.col("prix_moyen_m2") - F.col("prix_moyen_m2_precedent")) / F.col("prix_moyen_m2_precedent")) * 100,
                2
            )
        ).otherwise(F.lit(None))
    ).drop("prix_moyen_m2_precedent")

    dm = dm.select("annee_mois", "annee", "mois", "type_local", "prix_moyen_m2", "nb_ventes", "variation_pct_mois")

    log.info("dm_evolution_temporelle construit - {} lignes".format(dm.count()))
    return dm


# ---------------------------------------------
# DM 3 : dm_prix_par_densite
# ---------------------------------------------

def build_dm_prix_par_densite(df, log):
    log.info("Construction de dm_prix_par_densite")

    # Ajout de la tranche de population par commune
    df_tranches = df.withColumn(
        "tranche_population",
        F.when(F.col("population") < 1000, "Petite commune (< 1 000 hab)")
         .when(F.col("population") < 10000, "Bourg (1 000 - 10 000 hab)")
         .when(F.col("population") < 50000, "Ville moyenne (10 000 - 50 000 hab)")
         .when(F.col("population") < 100000, "Grande ville (50 000 - 100 000 hab)")
         .otherwise("Metropole (> 100 000 hab)")
    )

    # Agregation par tranche et type de bien
    dm = df_tranches.groupBy("tranche_population", "type_local") \
        .agg(
            F.countDistinct("code_insee_reconstruit").alias("nb_communes"),
            F.round(F.avg("prix_m2"), 2).alias("prix_moyen_m2"),
            F.round(F.expr("percentile_approx(prix_m2, 0.5)"), 2).alias("prix_median_m2"),
            F.count("*").alias("nb_transactions"),
            F.sum("population").alias("population_totale")
        )

    # Indicateur de tension du marche : transactions par habitant
    dm = dm.withColumn(
        "transactions_par_habitant",
        F.round(F.col("nb_transactions") / F.col("population_totale"), 6)
    )

    # Ordre logique des tranches
    dm = dm.withColumn(
        "ordre_tranche",
        F.when(F.col("tranche_population") == "Petite commune (< 1 000 hab)", 1)
         .when(F.col("tranche_population") == "Bourg (1 000 - 10 000 hab)", 2)
         .when(F.col("tranche_population") == "Ville moyenne (10 000 - 50 000 hab)", 3)
         .when(F.col("tranche_population") == "Grande ville (50 000 - 100 000 hab)", 4)
         .otherwise(5)
    )

    log.info("dm_prix_par_densite construit - {} lignes".format(dm.count()))
    return dm


# ---------------------------------------------
# DM 4 : dm_segmentation_biens
# ---------------------------------------------

def build_dm_segmentation_biens(df, log):
    log.info("Construction de dm_segmentation_biens")

    # Tranche de surface
    df_seg = df.withColumn(
        "tranche_surface",
        F.when(F.col("surface_reelle_bati") < 30, "Tres petit (< 30 m2)")
         .when(F.col("surface_reelle_bati") < 60, "Petit (30 - 60 m2)")
         .when(F.col("surface_reelle_bati") < 100, "Moyen (60 - 100 m2)")
         .when(F.col("surface_reelle_bati") < 150, "Grand (100 - 150 m2)")
         .otherwise("Tres grand (> 150 m2)")
    )

    # Agregation par type, tranche surface, nb pieces
    dm = df_seg.groupBy("type_local", "tranche_surface", "nombre_pieces_principales") \
        .agg(
            F.round(F.avg("valeur_fonciere"), 2).alias("valeur_fonciere_moyenne"),
            F.round(F.avg("prix_m2"), 2).alias("prix_m2_moyen"),
            F.count("*").alias("nb_biens")
        )

    # PERCENT_RANK sur le prix_m2_moyen par type de bien
    window_pct = Window.partitionBy("type_local").orderBy("prix_m2_moyen")
    dm = dm.withColumn(
        "prix_m2_percentile",
        F.round(F.percent_rank().over(window_pct), 4)
    )

    # NTILE(4) pour quartile
    window_ntile = Window.partitionBy("type_local").orderBy("prix_m2_moyen")
    dm = dm.withColumn(
        "quartile",
        F.ntile(4).over(window_ntile)
    )

    # Label de segment lisible
    dm = dm.withColumn(
        "segment_label",
        F.when(
            (F.col("type_local") == "Appartement") & (F.col("quartile") == 1), "Appartement entree de gamme"
        ).when(
            (F.col("type_local") == "Appartement") & (F.col("quartile") == 2), "Appartement milieu de gamme"
        ).when(
            (F.col("type_local") == "Appartement") & (F.col("quartile") == 3), "Appartement haut de gamme"
        ).when(
            (F.col("type_local") == "Appartement") & (F.col("quartile") == 4), "Appartement premium"
        ).when(
            (F.col("type_local") == "Maison") & (F.col("quartile") == 1), "Maison entree de gamme"
        ).when(
            (F.col("type_local") == "Maison") & (F.col("quartile") == 2), "Maison milieu de gamme"
        ).when(
            (F.col("type_local") == "Maison") & (F.col("quartile") == 3), "Maison haut de gamme"
        ).otherwise("Maison premium")
    )

    # Cle primaire segment_id
    dm = dm.withColumn(
        "segment_id",
        F.concat(
            F.col("type_local"),
            F.lit("_"),
            F.regexp_replace(F.col("tranche_surface"), " ", "_"),
            F.lit("_"),
            F.col("nombre_pieces_principales").cast("string")
        )
    )

    log.info("dm_segmentation_biens construit - {} lignes".format(dm.count()))
    return dm


# ---------------------------------------------
# MAIN
# ---------------------------------------------

def main():
    args = parse_args()

    config = configparser.ConfigParser()
    config.read(args.config)

    log_dir          = config["local"]["log_dir"]
    silver_path      = config["hdfs"]["silver_path"]
    hive_database    = config["hive"]["database"]
    hive_table       = config["hive"]["table_dvf_enrichi"]
    jdbc_url         = config["postgres"]["jdbc_url"]
    jdbc_driver      = config["postgres"]["jdbc_driver_path"]
    pg_user          = config["postgres"]["user"]
    pg_password      = config["postgres"]["password"]

    log = setup_logger(log_dir)
    log.info("=== Demarrage du datamart ===")

    spark = SparkSession.builder \
        .appName(config["spark"]["app_name_datamart"]) \
        .config("spark.sql.warehouse.dir", silver_path) \
        .config("spark.jars", jdbc_driver) \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        # Lecture depuis Hive silver
        log.info("Lecture de la table Hive : {}.{}".format(hive_database, hive_table))
        df = spark.sql("SELECT * FROM {}.{}".format(hive_database, hive_table))
        nb = df.count()
        log.info("Table silver chargee - {} lignes".format(nb))

        # Cache avant les 4 agregations - visible dans Spark UI
        df.cache()
        df.count()
        log.info("Cache applique sur silver")

        # DM 1
        dm1 = build_dm_prix_par_commune(df, log)
        write_to_postgres(dm1, "dm_prix_par_commune", jdbc_url, jdbc_driver, pg_user, pg_password, log)

        # DM 2
        dm2 = build_dm_evolution_temporelle(df, log)
        write_to_postgres(dm2, "dm_evolution_temporelle", jdbc_url, jdbc_driver, pg_user, pg_password, log)

        # DM 3
        dm3 = build_dm_prix_par_densite(df, log)
        write_to_postgres(dm3, "dm_prix_par_densite", jdbc_url, jdbc_driver, pg_user, pg_password, log)

        # DM 4
        dm4 = build_dm_segmentation_biens(df, log)
        write_to_postgres(dm4, "dm_segmentation_biens", jdbc_url, jdbc_driver, pg_user, pg_password, log)

        log.info("=== Datamart termine avec succes ===")

    except Exception as e:
        log.error("Erreur fatale dans le datamart : {}".format(e))
        spark.stop()
        sys.exit(1)

    spark.stop()


if __name__ == "__main__":
    main()
