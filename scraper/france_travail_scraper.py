# =============================================================================
#  scrapers/france_travail_scraper.py — Source : France Travail (API officielle)
#  Version SANS Kafka — écriture uniquement dans MinIO (bronze)
# =============================================================================

import sys
import os
import re
import uuid
from datetime import datetime

import requests

_SCRAPER_ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
if _SCRAPER_ROOT not in sys.path:
    sys.path.insert(0, _SCRAPER_ROOT)

from clients import get_minio_client
from helpers import save_to_minio
from config import FRANCE_TRAVAIL_CLIENT_ID, FRANCE_TRAVAIL_CLIENT_SECRET

# URLs API
TOKEN_URL  = "https://entreprise.francetravail.fr/connexion/oauth2/access_token"
OFFRES_URL = "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search"

# =============================================================================
#  REFERENTIEL COMPETENCES DATA ENGINEERING
# =============================================================================

COMPETENCES_DATA_ENG = {
    "langages":      ["python", "scala", "java", "sql", "bash", "pyspark", "r"],
    "frameworks":    ["spark", "kafka", "flink", "airflow", "dbt", "delta live tables", "databricks"],
    "cloud":         ["aws", "azure", "gcp", "bigquery", "snowflake", "dataflow", "adf", "adls",
                      "synapse", "glue", "redshift", "s3", "azure devops", "github actions"],
    "bases_donnees": ["postgresql", "mysql", "mongodb", "cassandra", "redis",
                      "elasticsearch", "hive", "clickhouse"],
    "outils":        ["docker", "kubernetes", "git", "gitlab", "terraform", "ci/cd", "minio", "hdfs", "hadoop"],
    "formats":       ["parquet", "avro", "delta lake", "iceberg", "lakehouse"],
    "bi_viz":        ["power bi", "tableau", "looker", "grafana", "superset"],
    "architecture":  ["dataops", "lakehouse", "data mesh", "data vault", "medallion", "lambda", "kappa"],
}

ALL_COMPETENCES = [c for groupe in COMPETENCES_DATA_ENG.values() for c in groupe]


# =============================================================================
#  NORMALISATION DES COMPETENCES
# =============================================================================

def extraire_et_normaliser(texte: str) -> dict:
    texte_lower = texte.lower()

    tags_normalises = {
        cat: [c for c in comps if c in texte_lower]
        for cat, comps in COMPETENCES_DATA_ENG.items()
        if any(c in texte_lower for c in comps)
    }

    tags_liste = [c for c in ALL_COMPETENCES if c in texte_lower]

    mots_connus = set(ALL_COMPETENCES)
    mots_texte  = set(re.findall(r'\b[a-zA-Z]{4,}\b', texte_lower))
    mots_exclus = {
        "avec", "dans", "pour", "plus", "tres", "bonnes", "avoir",
        "entre", "sans", "sous", "votre", "notre", "cette", "tout",
        "vous", "nous", "leur", "sera", "sont", "tous", "etre",
    }
    nouveaux = [
        m for m in mots_texte
        if m not in mots_connus and m not in mots_exclus
    ][:10]

    return {
        "competences_normalisees": tags_normalises,
        "competences_liste":       tags_liste,
        "competences_nouvelles":   nouveaux,
    }


# =============================================================================
#  FILTRE DATA ENGINEERING
# =============================================================================

def is_data_engineering_job(title: str, description: str = "") -> bool:
    keywords = [
        "data engineer", "data engineering", "ingenieur data", "big data",
        "etl", "pipeline", "spark", "dbt", "data platform", "lakehouse", "databricks",
    ]
    exclude = ["commercial", "comptable", "assistante", "vente"]
    texte = (title + " " + description).lower()
    if any(e in texte for e in exclude):
        return False
    return any(k in texte for k in keywords)


# =============================================================================
#  AUTHENTIFICATION OAuth2
# =============================================================================

def _get_token() -> str:
    client_id = FRANCE_TRAVAIL_CLIENT_ID
    client_secret = FRANCE_TRAVAIL_CLIENT_SECRET
    if not client_id or not client_secret:
        raise ValueError("FRANCE_TRAVAIL_CLIENT_ID et FRANCE_TRAVAIL_CLIENT_SECRET manquants")

    resp = requests.post(
        TOKEN_URL,
        params={"realm": "/partenaire"},
        data={
            "grant_type":    "client_credentials",
            "client_id":     client_id,
            "client_secret": client_secret,
            "scope":         "api_offresdemploiv2 o2dsoffre",
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=15,
    )

    if resp.status_code != 200:
        print("[FranceTravail] Reponse auth: " + resp.text[:300])
        resp.raise_for_status()

    print("[FranceTravail] Token obtenu")
    return resp.json().get("access_token", "")


# =============================================================================
#  SCRAPER PRINCIPAL
# =============================================================================

def _scrape_france_travail(keywords: str = "data engineer", max_results: int = 50) -> list:
    print("[FranceTravail] Recherche : '" + keywords + "' | Max : " + str(max_results))

    try:
        token = _get_token()
    except Exception as e:
        print("[FranceTravail] Erreur authentification : " + str(e))
        return []

    headers  = {"Authorization": "Bearer " + token, "Accept": "application/json"}
    records  = []
    start    = 0
    per_page = min(50, max_results)

    while len(records) < max_results:
        params = {
            "motsCles": keywords,
            "range":    f"{start}-{start + per_page - 1}",
            "sort":     "1",
        }

        try:
            resp = requests.get(OFFRES_URL, headers=headers, params=params, timeout=15)
            if resp.status_code == 204:
                print("[FranceTravail] Aucune offre trouvee")
                break
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print("[FranceTravail] Erreur HTTP " + str(resp.status_code) + " : " + str(e))
            print("[FranceTravail] Reponse : " + resp.text[:200])
            break
        except requests.exceptions.RequestException as e:
            print("[FranceTravail] Erreur reseau : " + str(e))
            break

        offres = resp.json().get("resultats", [])
        if not offres:
            print("[FranceTravail] Fin des resultats a l'offset " + str(start))
            break

        print("[FranceTravail] Offset " + str(start) + " : " + str(len(offres)) + " offre(s)")

        for offre in offres:
            title       = offre.get("intitule", "")
            description = offre.get("description", "")

            # Normalisation des compétences
            comp = extraire_et_normaliser(title + " " + description)

            lieu       = offre.get("lieuTravail", {})
            entreprise = offre.get("entreprise", {})
            salaire    = offre.get("salaire", {})
            ref        = offre.get("id", "")
            url        = offre.get("origineOffre", {}).get("urlOrigine", "")
            if not url:
                url = f"https://candidat.francetravail.fr/offres/recherche/detail/{ref}"

            records.append({
                "id":                      str(uuid.uuid4()),
                "source":                  "francetravail.fr",
                "title":                   title,
                "company":                 entreprise.get("nom", "Non precise"),
                "location":                lieu.get("libelle", "France"),
                "contract_type":           offre.get("typeContratLibelle", ""),
                "salary":                  salaire.get("libelle", ""),
                "description":             description[:500],
                "url":                     url,
                "published_at":            offre.get("dateCreation", ""),
                "scraped_at":              datetime.now().isoformat(),
                "country":                 "France",
                "competences_normalisees": comp["competences_normalisees"],
                "competences_liste":       comp["competences_liste"],
                "competences_nouvelles":   comp["competences_nouvelles"],
                "experience":              offre.get("experienceLibelle", ""),
                "niveau_etude":            offre.get("niveauRequis", {}).get("libelle", ""),
                "secteur_activite":        offre.get("secteurActiviteLibelle", ""),
                "code_rome":               offre.get("romeCode", ""),
            })

            if len(records) >= max_results:
                break

        content_range = resp.headers.get("Content-Range", "")
        if content_range:
            try:
                total = int(content_range.split("/")[-1])
                if start + per_page >= total:
                    break
            except Exception:
                pass

        start += per_page

    print("[FranceTravail] " + str(len(records)) + " offre(s) extraite(s)")
    return records


# =============================================================================
#  PIPELINE COMPLET (UNIQUEMENT MINIO)
# =============================================================================

def run_france_travail(keywords: str = "data engineer", max_results: int = 50) -> dict:
    """
    Scrape France Travail et sauvegarde dans MinIO (bronze). Pas de Kafka.
    """
    records = _scrape_france_travail(keywords, max_results)
    if not records:
        return {"scraped": 0, "message": "Aucune offre trouvee sur France Travail"}

    minio_client = get_minio_client()
    minio_path = save_to_minio(minio_client, records, source="france_travail")

    print(f"\n✅ {len(records)} offres → MinIO bucket 'raw' path : {minio_path}")
    return {"scraped": len(records), "minio_path": minio_path}


# =============================================================================
#  POINT D'ENTREE
# =============================================================================

if __name__ == "__main__":
    import json
    result = run_france_travail("data engineer", max_results=20)
    print(json.dumps(result, ensure_ascii=False, indent=2))