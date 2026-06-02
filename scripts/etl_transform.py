# scripts/etl_transform.py
# Nettoyage, dédoublonnage, extraction compétences, écriture Silver/Gold en JSON

import hashlib
import json
import re
import uuid
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import pandas as pd

from clients import get_minio_client
from config import MINIO_BUCKET_SILVER, MINIO_BUCKET_GOLD

# ------------------------------------------------------------------
# Référentiel compétences (identique à votre scraper)
# ------------------------------------------------------------------
COMPETENCES_DATA_ENG = {
    "langages": ["python", "scala", "java", "sql", "r", "bash"],
    "big_data": ["spark", "hadoop", "kafka", "flink", "databricks"],
    "etl": ["etl", "elt", "data pipeline", "workflow", "orchestration"],
    "cloud": ["aws", "azure", "gcp", "s3", "snowflake", "bigquery"],
    "data": ["data warehouse", "data lake", "lakehouse", "datamart"],
    "bi": ["power bi", "tableau", "looker"],
    "devops": ["docker", "kubernetes", "git", "ci/cd"],
    "ml": ["machine learning", "ml", "ai", "data science"]
}
ALL_COMPETENCES = [c for g in COMPETENCES_DATA_ENG.values() for c in g]
STOPWORDS = {"avec","dans","pour","plus","vous","nous","entre","poste","profil"}

def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def contains_exact_word(text: str, word: str) -> bool:
    return re.search(rf"\b{re.escape(word)}\b", text) is not None

def extract_competences_from_text(text: str) -> Dict:
    text_clean = clean_text(text)
    normalisees = {}
    for cat, skills in COMPETENCES_DATA_ENG.items():
        found = [s for s in skills if contains_exact_word(text_clean, s)]
        if found:
            normalisees[cat] = found
    competences_liste = [s for s in ALL_COMPETENCES if contains_exact_word(text_clean, s)]
    words = set(re.findall(r"\b[a-zA-Z]{4,}\b", text_clean))
    nouveaux = [w for w in words if w not in ALL_COMPETENCES and w not in STOPWORDS][:10]
    return {
        "competences_liste": competences_liste,
        "competences_normalisees": normalisees,
        "competences_nouvelles": nouveaux,
    }

def parse_contract_type(ct: Optional[str]) -> str:
    if not ct:
        return "Non précisé"
    ct = ct.lower()
    if "cdi" in ct: return "CDI"
    if "cdd" in ct: return "CDD"
    if "freelance" in ct: return "Freelance"
    if "stage" in ct: return "Stage"
    if "alternance" in ct: return "Alternance"
    return "Autre"

def parse_experience(description: str) -> str:
    desc = clean_text(description)
    if "junior" in desc or "débutant" in desc:
        return "Junior (0-2 ans)"
    if "confirmé" in desc or "3-5" in desc:
        return "Confirmé (3-5 ans)"
    if "senior" in desc or "expert" in desc:
        return "Senior (5+ ans)"
    return "Non spécifié"

def parse_remote(description: str) -> str:
    return "Oui" if "télétravail" in clean_text(description) else "Non"

def generate_offer_hash(offre: Dict) -> str:
    unique_str = f"{offre.get('title','')}|{offre.get('company','')}|{offre.get('location','')}|{offre.get('description','')[:200]}"
    return hashlib.md5(unique_str.encode("utf-8")).hexdigest()

def transformer_offre(raw_offre: Dict) -> Optional[Dict]:
    if not raw_offre.get("title") or not raw_offre.get("description"):
        return None
    description = raw_offre.get("description", "")
    comp = extract_competences_from_text(description)
    scraped_at = raw_offre.get("scraped_at") or datetime.now().isoformat()
    try:
        scraped_at_dt = datetime.fromisoformat(scraped_at.replace("Z", "+00:00"))
    except:
        scraped_at_dt = datetime.now()
    return {
        "id": generate_offer_hash(raw_offre),
        "source": raw_offre.get("source", "unknown"),
        "title": raw_offre.get("title", "").strip(),
        "company": raw_offre.get("company", "").strip(),
        "location": raw_offre.get("location", "") or raw_offre.get("localisation_detail", "") or "Non précisé",
        "description": description,
        "missions": raw_offre.get("missions", description[:500]),
        "profil": raw_offre.get("profil", ""),
        "link": raw_offre.get("url") or raw_offre.get("link", ""),
        "scraped_at": scraped_at_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "date_limite": raw_offre.get("date_limite"),
        "type_contrat": parse_contract_type(raw_offre.get("contract_type")),
        "niveau_experience": parse_experience(description),
        "niveau_etude": raw_offre.get("niveau_etude", "Non précisé"),
        "teletravail": parse_remote(description),
        "salaire": raw_offre.get("salary"),
        "competences_liste": comp["competences_liste"],
        "competences_normalisees": comp["competences_normalisees"],
        "competences_nouvelles": comp["competences_nouvelles"],
    }

def dedupliquer(offres: List[Dict]) -> Tuple[List[Dict], int]:
    vus = set()
    uniques = []
    for o in offres:
        h = o["id"]
        if h not in vus:
            vus.add(h)
            uniques.append(o)
    return uniques, len(offres) - len(uniques)

# ------------------------------------------------------------------
# Écriture MinIO Silver / Gold en JSON (au lieu de Parquet)
# ------------------------------------------------------------------
def save_silver_minio(offres: List[Dict], source: str):
    if not offres:
        return
    df = pd.DataFrame(offres)
    # Convertir les colonnes complexes en chaînes JSON
    for col in ["competences_normalisees", "competences_nouvelles"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False))
    minio_client = get_minio_client()
    date_str = datetime.now().strftime("%Y-%m-%d")
    # Utilisation de l'extension .json
    file_name = f"silver/source={source}/date={date_str}/jobs_{uuid.uuid4()}.json"
    tmp_path = f"/tmp/{uuid.uuid4()}.json"
    # Sauvegarde en JSON
    df.to_json(tmp_path, orient='records', force_ascii=False, lines=True)
    minio_client.fput_object(MINIO_BUCKET_SILVER, file_name, tmp_path)
    print(f"💾 Silver {source} → {file_name}")

def save_gold_minio(offres: List[Dict]):
    if not offres:
        return
    gold_df = pd.DataFrame([{
        "id_offre": o["id"],
        "titre": o["title"],
        "entreprise": o["company"],
        "ville": o["location"],
        "type_contrat": o["type_contrat"],
        "teletravail": o["teletravail"],
        "competences": ", ".join(o["competences_liste"]),
        "url": o["link"],
        "date_scraping": o["scraped_at"]
    } for o in offres])
    minio_client = get_minio_client()
    date_str = datetime.now().strftime("%Y-%m-%d")
    file_name = f"gold/recommandations_{date_str}_{uuid.uuid4()}.json"
    tmp_path = f"/tmp/{uuid.uuid4()}.json"
    gold_df.to_json(tmp_path, orient='records', force_ascii=False, lines=True)
    minio_client.fput_object(MINIO_BUCKET_GOLD, file_name, tmp_path)
    print(f"📊 Gold → {file_name}")