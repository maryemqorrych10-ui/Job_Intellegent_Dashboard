# =============================================================================
#  scraper/helpers.py — Fonctions utilitaires partagées
#  - Publication Kafka
#  - Sauvegarde MinIO
#  - Normalisation JobSpy
#  - Filtre Data Engineering
# =============================================================================

import io
import json
import uuid
from datetime import datetime

import pandas as pd
from confluent_kafka import Producer
from minio import Minio

from config import DATA_ENGINEERING_KEYWORDS, MINIO_BUCKET


# ─────────────────────────────────────────────────────────────────────────────
#  Kafka
# ─────────────────────────────────────────────────────────────────────────────

def publish_to_kafka(producer: Producer, topic: str, records: list) -> None:
    """Publie une liste de records JSON dans un topic Kafka."""
    for record in records:
        producer.produce(
            topic,
            key=str(record.get("id", uuid.uuid4())),
            value=json.dumps(record, ensure_ascii=False, default=str),
        )
    producer.flush()


# ─────────────────────────────────────────────────────────────────────────────
#  MinIO
# ─────────────────────────────────────────────────────────────────────────────

def save_to_minio(client: Minio, data: list, source: str) -> str:
    """
    Sérialise `data` en JSON et le stocke dans MinIO.
    Retourne le chemin de l'objet créé.
    Chemin : <source>/<YYYY-MM-DD>/<uuid>.json
    """
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)

    date_str    = datetime.now().strftime("%Y-%m-%d")
    object_name = f"{source}/{date_str}/{uuid.uuid4()}.json"
    content     = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")

    client.put_object(
        MINIO_BUCKET,
        object_name,
        data=io.BytesIO(content),
        length=len(content),
        content_type="application/json",
    )
    return object_name


# ─────────────────────────────────────────────────────────────────────────────
#  Normalisation JobSpy
# ─────────────────────────────────────────────────────────────────────────────

def normalize_jobspy(df: pd.DataFrame) -> list:
    """
    Convertit le DataFrame renvoyé par JobSpy en liste de dicts normalisés
    (format commun à toutes les sources du projet).
    """
    records = []
    for _, row in df.iterrows():
        records.append({
            "id":            str(uuid.uuid4()),
            "source":        str(row.get("site", "jobspy")),
            "title":         str(row.get("title", "")),
            "company":       str(row.get("company", "")),
            "location":      str(row.get("location", "")),
            "contract_type": str(row.get("job_type", "")),
            "salary":        f"{row.get('min_amount', '')} - {row.get('max_amount', '')}",
            "description":   str(row.get("description", "")),
            "url":           str(row.get("job_url", "")),
            "published_at":  str(row.get("date_posted", "")),
            "scraped_at":    datetime.now().isoformat(),
            "country":       "France",
        })
    return records
# helpers.py (ajout)
def read_from_minio(minio_client, bucket, prefix):
    """Lit tous les fichiers JSON depuis un préfixe MinIO et retourne la liste des offres."""
    objects = minio_client.list_objects(bucket, prefix=prefix, recursive=True)
    all_offres = []
    for obj in objects:
        response = minio_client.get_object(bucket, obj.object_name)
        data = json.loads(response.read().decode("utf-8"))
        response.close()
        if isinstance(data, list):
            all_offres.extend(data)
        else:
            all_offres.append(data)
    return all_offres

# ─────────────────────────────────────────────────────────────────────────────
#  Filtre Data Engineering
# ─────────────────────────────────────────────────────────────────────────────

def is_data_engineering_offer(title: str, description: str = "") -> bool:
    """
    Retourne True si le titre ou la description contient au moins un mot-clé
    lié au Data Engineering (liste définie dans config.py).
    Utilisé pour filtrer toutes les sources marocaines.
    """
    text = (title + " " + description).lower()
    return any(kw in text for kw in DATA_ENGINEERING_KEYWORDS)
