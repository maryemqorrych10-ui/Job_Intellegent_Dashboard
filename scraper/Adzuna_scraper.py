# =============================================================================
#  scrapers/adzuna_scraper.py — VERSION SANS KAFKA (uniquement MinIO)
# =============================================================================

import uuid
import re
from datetime import datetime

import requests

from clients import get_minio_client
from helpers import save_to_minio
from config import ADZUNA_APP_ID, ADZUNA_APP_KEY


# =============================================================================
#  RÉFÉRENTIEL COMPÉTENCES
# =============================================================================
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

ALL_COMPETENCES = [
    c for groupe in COMPETENCES_DATA_ENG.values() for c in groupe
]


# =============================================================================
#  UTILS
# =============================================================================
def clean_text(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    return text


def contains_word(text: str, word: str) -> bool:
    """
    🔥 détection mot EXACT (évite problème 'r')
    """
    return re.search(rf"\b{re.escape(word)}\b", text) is not None


# =============================================================================
#  EXTRACTION COMPÉTENCES
# =============================================================================
def extract_competences(text: str) -> dict:
    texte = clean_text(text)

    # --- normalisées par catégorie ---
    competences_normalisees = {}
    for cat, skills in COMPETENCES_DATA_ENG.items():
        found = [s for s in skills if contains_word(texte, s)]
        if found:
            competences_normalisees[cat] = found

    # --- liste plate ---
    competences_liste = [
        s for s in ALL_COMPETENCES if contains_word(texte, s)
    ]

    # --- nouveaux mots ---
    mots = set(re.findall(r"\b[a-zA-Z]{4,}\b", texte))
    connus = set(ALL_COMPETENCES)

    stopwords = {
        "avec", "dans", "pour", "plus", "vous", "nous",
        "entre", "leurs", "elles", "notre", "votre",
        "poste", "profil", "entreprise", "mission"
    }

    nouveaux = [
        m for m in mots if m not in connus and m not in stopwords
    ][:10]

    return {
        "competences_brutes": text[:2000],
        "competences_normalisees": competences_normalisees,
        "competences_liste": competences_liste,
        "competences_nouvelles": nouveaux,
    }


# =============================================================================
#  SCRAPING ADZUNA
# =============================================================================
def _scrape_adzuna(keywords: str, max_results: int) -> list:

    if not ADZUNA_APP_ID or not ADZUNA_APP_KEY:
        print("[Adzuna] API KEY manquante")
        return []

    pages = max(1, max_results // 20)
    records = []

    for page in range(1, pages + 1):

        url = (
            f"https://api.adzuna.com/v1/api/jobs/fr/search/{page}"
            f"?app_id={ADZUNA_APP_ID}&app_key={ADZUNA_APP_KEY}"
            f"&results_per_page=20&what={keywords.replace(' ', '+')}"
        )

        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"[Adzuna] erreur page {page} : {e}")
            break

        jobs = resp.json().get("results", [])

        for job in jobs:

            # 🔥 TEXTE COMPLET (IMPORTANT)
            full_text = f"{job.get('title', '')} {job.get('description', '')}"

            # 🔥 EXTRACTION COMPÉTENCES
            comp = extract_competences(full_text)

            record = {
                "id": str(uuid.uuid4()),
                "source": "adzuna",
                "title": job.get("title", ""),
                "company": job.get("company", {}).get("display_name", ""),
                "location": job.get("location", {}).get("display_name", ""),
                "contract_type": job.get("contract_type", ""),
                "salary": f"{job.get('salary_min', '')} - {job.get('salary_max', '')}",
                "description": job.get("description", ""),
                "url": job.get("redirect_url", ""),
                "published_at": job.get("created", ""),
                "scraped_at": datetime.now().isoformat(),
                "country": "France",

                # 🔥 COMPÉTENCES AJOUTÉES
                **comp
            }

            records.append(record)

    return records


# =============================================================================
#  PIPELINE (uniquement MinIO)
# =============================================================================
def _run_adzuna(keywords: str, max_results: int):

    records = _scrape_adzuna(keywords, max_results)

    if not records:
        return {"scraped": 0}

    minio = get_minio_client()
    path = save_to_minio(minio, records, source="adzuna")

    return {
        "scraped": len(records),
        "minio_path": path
    }


# =============================================================================
#  MAIN
# =============================================================================
if __name__ == "__main__":
    result = _run_adzuna("data engineer", 100)
    print(result)