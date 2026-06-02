# dags/job_intelligent_pipeline.py
from __future__ import annotations

import os
import sys
import time
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# =============================================================================
# CONFIGURATION
# =============================================================================

SCRAPER_API_URL = os.getenv("SCRAPER_API_URL", "http://scraper-api:8000")
SCRAPE_KEYWORDS = "data engineer"
SCRAPE_MAX = 500

DEFAULT_ARGS = {
    "owner": "job_intelligent",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# =============================================================================
# HELPER APPEL API SCRAPER
# =============================================================================
def _call_scraper(endpoint: str, payload: dict, source_name: str) -> dict:
    url = SCRAPER_API_URL + endpoint
    print(f"[Scraping] POST {url}")
    try:
        resp = requests.post(url, json=payload, timeout=30)
        resp.raise_for_status()
        result = resp.json()
    except Exception as e:
        print(f"[Scraping] {source_name} erreur : {e}")
        result = {"status": "erreur", "message": str(e)}
    print(f"[Scraping] {source_name} : {result}")
    return result

# =============================================================================
# TÂCHES DE SCRAPING
# =============================================================================
def task_scrape_rekrute(**context):
    result = _call_scraper("/scrape/rekrute", {"keywords": SCRAPE_KEYWORDS, "max_results": SCRAPE_MAX}, "Rekrute")
    context["ti"].xcom_push(key="rekrute_status", value=result)
    return result

def task_scrape_france_travail(**context):
    result = _call_scraper("/scrape/france_travail", {"keywords": SCRAPE_KEYWORDS, "max_results": SCRAPE_MAX}, "FranceTravail")
    context["ti"].xcom_push(key="france_travail_status", value=result)
    return result

def task_scrape_adzuna(**context):
    result = _call_scraper("/scrape/adzuna", {"keywords": SCRAPE_KEYWORDS, "max_results": SCRAPE_MAX}, "Adzuna")
    context["ti"].xcom_push(key="adzuna_status", value=result)
    return result

def task_wait_scraping(**context):
    print("[Wait] Attente 10s pour laisser les scrapers écrire dans MinIO...")
    time.sleep(10)

# =============================================================================
# TÂCHE ETL (lecture depuis MinIO bronze)
# =============================================================================
def task_etl_minio(**context):
    sys.path.insert(0, "/opt/airflow/scripts")
    from etl_from_minio import run_etl_from_minio
    result = run_etl_from_minio(batch_size=100)
    context["ti"].xcom_push(key="etl_total_lus", value=result.get("total_lus", 0))
    context["ti"].xcom_push(key="etl_total_charge", value=result.get("total_charge", 0))
    return result

# =============================================================================
# TÂCHE CHARGEMENT MYSQL (lecture depuis MinIO gold) – optionnel (car déjà fait dans l'ETL)
# =============================================================================
def task_load_star_schema(**context):
    sys.path.insert(0, "/opt/airflow/scripts")
    from load_star_schema import load_offres_to_star_schema
    from minio import Minio
    import json

    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000").replace("http://", "")
    MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

    minio_client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)
    sources = ["rekrute", "france_travail", "adzuna"]
    all_offres = []

    for src in sources:
        try:
            objects = minio_client.list_objects("gold", prefix=src + "/", recursive=True)
            for obj in objects:
                data = minio_client.get_object("gold", obj.object_name)
                content = json.loads(data.read().decode("utf-8"))
                data.close()
                if isinstance(content, list):
                    all_offres.extend(content)
                else:
                    all_offres.append(content)
        except Exception as e:
            print(f"[Load] Erreur source {src}: {e}")

    print(f"[Load] {len(all_offres)} offres lues depuis MinIO gold")
    result = load_offres_to_star_schema(all_offres) if all_offres else {"charge": 0}
    context["ti"].xcom_push(key="load_result", value=result)
    return result

# =============================================================================
# VÉRIFICATION QUALITÉ
# =============================================================================
def task_check_quality(**context):
    ti = context["ti"]
    etl_lus = ti.xcom_pull(key="etl_total_lus", task_ids="etl_minio") or 0
    etl_charge = ti.xcom_pull(key="etl_total_charge", task_ids="etl_minio") or 0
    load_result = ti.xcom_pull(key="load_result", task_ids="load_star_schema") or {}
    total_charge = load_result.get("charge", 0)
    total_doublons = load_result.get("doublons", 0)
    total_erreurs = load_result.get("erreurs", 0)

    print("=" * 55)
    print("  RAPPORT PIPELINE JOB INTELLIGENT")
    print("=" * 55)
    print(f"  Rekrute        : {ti.xcom_pull(key='rekrute_status', task_ids='scrape_rekrute')}")
    print(f"  France Travail : {ti.xcom_pull(key='france_travail_status', task_ids='scrape_france_travail')}")
    print(f"  Adzuna         : {ti.xcom_pull(key='adzuna_status', task_ids='scrape_adzuna')}")
    print(f"  Offres brutes lues depuis MinIO : {etl_lus}")
    print(f"  ETL transformés : {etl_charge}")
    print(f"  MySQL chargées  : {total_charge}")
    print(f"  Doublons        : {total_doublons}")
    print(f"  Erreurs         : {total_erreurs}")
    print("=" * 55)

    if total_erreurs > 0 and total_charge == 0:
        raise ValueError("Pipeline échoué : 0 offre chargée, erreurs présentes")
    return {"status": "success"}

# =============================================================================
# DÉFINITION DU DAG
# =============================================================================
with DAG(
    dag_id="job_intelligent_pipeline_simplified",
    description="Scraping -> MinIO bronze -> ETL -> MinIO silver/gold -> MySQL",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["job_intelligent", "etl", "no-kafka"],
) as dag:

    start = EmptyOperator(task_id="start")
    scrape_rekrute = PythonOperator(task_id="scrape_rekrute", python_callable=task_scrape_rekrute)
    scrape_france_travail = PythonOperator(task_id="scrape_france_travail", python_callable=task_scrape_france_travail)
    scrape_adzuna = PythonOperator(task_id="scrape_adzuna", python_callable=task_scrape_adzuna)
    wait_scraping = PythonOperator(task_id="wait_scraping", python_callable=task_wait_scraping, trigger_rule=TriggerRule.ALL_DONE)
    etl_minio = PythonOperator(task_id="etl_minio", python_callable=task_etl_minio, execution_timeout=timedelta(minutes=15))
    load_star_schema = PythonOperator(task_id="load_star_schema", python_callable=task_load_star_schema, execution_timeout=timedelta(minutes=10))
    check_quality = PythonOperator(task_id="check_quality", python_callable=task_check_quality, trigger_rule=TriggerRule.ALL_DONE)
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    start >> [scrape_rekrute, scrape_france_travail, scrape_adzuna] >> wait_scraping
    wait_scraping >> etl_minio >> load_star_schema >> check_quality >> end