# =============================================================================
#  scraper/config.py — Configuration globale
#  Variables d'environnement, constantes, headers HTTP
# =============================================================================

import os

# ── Kafka ─────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS",  "kafka:9092")
KAFKA_TOPIC_ADZUNA        = os.getenv("KAFKA_TOPIC_ADZUNA",        "raw_jobs_adzuna")
KAFKA_TOPIC_REKRUTE       = os.getenv("KAFKA_TOPIC_REKRUTE",       "raw_jobs_rekrute")
KAFKA_TOPIC_FRANCE_TRAVAIL = os.getenv("KAFKA_TOPIC_FRANCE_TRAVAIL", "raw_jobs_france_travail")



# ── MinIO ─────────────────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000").replace("http://", "").replace("https://", "")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET   = os.getenv("MINIO_BUCKET_RAW", "raw")
MINIO_BUCKET_RAW    = MINIO_BUCKET   # alias pour raw
MINIO_BUCKET_SILVER = os.getenv("MINIO_BUCKET_SILVER", "silver")
MINIO_BUCKET_GOLD   = os.getenv("MINIO_BUCKET_GOLD", "gold")

# ── Adzuna API ────────────────────────────────────────────────────────────────
ADZUNA_APP_ID  = os.getenv("ADZUNA_APP_ID",  "fa0a9ede")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY", "af675a335b7180721a377f36a8c69ec8")
# ── france_travail API ────────────────────────────────────────────────────────────────
FRANCE_TRAVAIL_CLIENT_ID = os.getenv("FRANCE_TRAVAIL_CLIENT_ID")
FRANCE_TRAVAIL_CLIENT_SECRET = os.getenv("FRANCE_TRAVAIL_CLIENT_SECRET")

# ── Mots-clés Data Engineering — filtre pour sources marocaines ──────────────
DATA_ENGINEERING_KEYWORDS = [
    "data engineer", "data engineering", "ingénieur data", "ingénieur de données",
    "etl", "pipeline de données", "pipeline data", "data pipeline",
    "apache spark", "apache kafka", "hadoop", "airflow", "dbt",
    "data warehouse", "data lake", "lakehouse", "databricks",
    "snowflake", "bigquery", "redshift", "azure data factory",
    "python data", "sql avancé", "orchestration", "ingestion de données",
    "architect data", "architecte data", "cloud data", "aws data",
    "azure data", "gcp data",
]

# ── Headers HTTP anti-bot ─────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ── MySQL (schéma en étoile) ─────────────────────────────────────────
MYSQL_HOST     = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT     = os.getenv("MYSQL_PORT", "3306")
MYSQL_USER     = os.getenv("MYSQL_USER", "jobs_user")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "jobspassword")
MYSQL_DATABASE = os.getenv("MYSQL_DB", "jobs_db")

