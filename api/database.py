# -*- coding: utf-8 -*-
"""
database.py - Connexion PostgreSQL et lecture paginee des datamarts.

Aucun parametre n'est code en dur : tout est lu depuis config.ini ([api]).
"""

import configparser

import psycopg2
import psycopg2.extras


class Database:
    """Gere la connexion a PostgreSQL et fournit la lecture paginee."""

    def __init__(self, config_path):
        config = configparser.ConfigParser()
        config.read(config_path)
        api = config["api"]

        # Parametres de connexion lus depuis config.ini
        self.conn_params = {
            "host": api.get("db_host", "localhost"),
            "port": api.get("db_port", "5433"),
            "dbname": api.get("db_name", "immo_datamarts"),
            "user": api.get("db_user", "immo_user"),
            "password": api.get("db_password", "immo_pass"),
        }

    def get_connection(self):
        """Ouvre une nouvelle connexion PostgreSQL."""
        return psycopg2.connect(**self.conn_params)

    def fetch_paginated(self, table, order_by, page, page_size):
        """
        Lit une page d'un datamart.

        IMPORTANT - securite : `table` et `order_by` proviennent d'une map
        fixe cote serveur (cf. app.py), JAMAIS de l'utilisateur. Il n'y a
        donc aucun risque d'injection SQL ici. `page` et `page_size` sont
        des entiers valides par FastAPI.

        Retourne un tuple (lignes, nombre_total_de_lignes).
        """
        offset = (page - 1) * page_size
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                # Nombre total de lignes (pour calculer le nombre de pages)
                cur.execute("SELECT COUNT(*) AS total FROM {}".format(table))
                total = cur.fetchone()["total"]

                # Page demandee, triee de maniere deterministe
                cur.execute(
                    "SELECT * FROM {} ORDER BY {} LIMIT %s OFFSET %s".format(
                        table, order_by
                    ),
                    (page_size, offset),
                )
                rows = cur.fetchall()

            return rows, total
        finally:
            conn.close()
