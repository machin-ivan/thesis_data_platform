import os


PSQL_DB = os.getenv("PSQL_DB", "airflow")
PSQL_USER = os.getenv("PSQL_USER", "airflow")
PSQL_PASSWORD = os.getenv("PSQL_PASSWORD", "airflow")
PSQL_PORT = os.getenv("PSQL_PORT", 5432)

conn_params = {'host': "postgres",
               'database': PSQL_DB,
               'port': PSQL_PORT,
               'user': PSQL_USER,
               'password': PSQL_PASSWORD}