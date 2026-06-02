# =============================================================================
#  scrapers/scraper_recruteAutre.py — VERSION SANS KAFKA (uniquement MinIO)
# =============================================================================

import time
import uuid
import re
import os
import json
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from clients import get_minio_client
from helpers import save_to_minio

# =============================================================================
#  DRIVER
# =============================================================================
def _get_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.binary_location = os.getenv("CHROME_BIN", "/usr/bin/chromium")
    service = Service(
        executable_path=os.getenv("CHROMEDRIVER_PATH", "/usr/bin/chromedriver")
    )
    print(f"✅ Chromium    : {options.binary_location}")
    print(f"✅ Chromedriver: {os.getenv('CHROMEDRIVER_PATH', '/usr/bin/chromedriver')}")
    return webdriver.Chrome(service=service, options=options)


# =============================================================================
#  RÉFÉRENTIEL COMPÉTENCES
# =============================================================================
COMPETENCES_DATA_ENG = {
    "langages":      ["python", "scala", "java", "sql", "bash", "pyspark", "r"],
    "frameworks":    ["spark", "kafka", "flink", "airflow", "dbt",
                      "delta live tables", "databricks"],
    "cloud":         ["aws", "azure", "gcp", "bigquery", "snowflake",
                      "dataflow", "adf", "adls", "synapse", "glue",
                      "redshift", "s3", "azure devops", "github actions"],
    "bases_donnees": ["postgresql", "mysql", "mongodb", "cassandra",
                      "redis", "elasticsearch", "hive", "clickhouse"],
    "outils":        ["docker", "kubernetes", "git", "gitlab", "terraform",
                      "ci/cd", "minio", "hdfs", "hadoop"],
    "formats":       ["parquet", "avro", "delta lake", "iceberg", "lakehouse"],
    "bi_viz":        ["power bi", "tableau", "looker", "grafana", "superset"],
    "architecture":  ["dataops", "lakehouse", "data mesh", "data vault",
                      "medallion", "lambda", "kappa"],
}

ALL_COMPETENCES = [c for groupe in COMPETENCES_DATA_ENG.values() for c in groupe]


# =============================================================================
#  EXTRACTION + NORMALISATION COMPÉTENCES
# =============================================================================
def extraire_et_normaliser(items_site: list) -> dict:
    """
    - competences_brutes    : texte exact du site (audit/traçabilité)
    - competences_normalisees: tags structurés par catégorie (Power BI)
    - competences_liste     : liste plate (recommandation)
    - competences_nouvelles : mots non reconnus (enrichissement futur)
    """
    texte_complet = " ".join(items_site).lower()

    tags_normalises = {
        cat: [c for c in comps if c in texte_complet]
        for cat, comps in COMPETENCES_DATA_ENG.items()
        if any(c in texte_complet for c in comps)
    }

    tags_liste = [c for c in ALL_COMPETENCES if c in texte_complet]

    mots_connus = set(ALL_COMPETENCES)
    mots_site   = set(re.findall(r'\b[a-zA-Z]{4,}\b', texte_complet))
    nouveaux    = [
        m for m in mots_site
        if m not in mots_connus
        and m not in {"avec", "dans", "pour", "plus", "très",
                      "bonnes", "avoir", "entre", "sans", "sous",
                      "votre", "notre", "cette", "tout", "vous"}
    ][:10]

    return {
        "competences_brutes":     items_site,
        "competences_normalisees": tags_normalises,
        "competences_liste":       tags_liste,
        "competences_nouvelles":   nouveaux,
    }


# =============================================================================
#  FILTRE DATA ENGINEERING
# =============================================================================
def is_data_engineering_job(title: str, description: str = "") -> bool:
    keywords = [
        "data engineer", "data engineering", "ingénieur data",
        "big data", "etl", "pipeline", "spark", "dbt",
        "data platform", "lakehouse", "databricks"
    ]
    exclude = ["commercial", "comptable", "assistante", "stage", "vente"]
    texte = (title + " " + description).lower()
    if any(e in texte for e in exclude):
        return False
    return any(k in texte for k in keywords)


# =============================================================================
#  HELPERS
# =============================================================================
def _extract_location(title: str) -> str:
    match = re.search(r'\|\s*(.+?)\s*\(', title)
    return match.group(1).strip() if match else "Maroc"


def _get_bloc_by_h2(driver, keyword: str):
    """Retourne le texte et les <li> du bloc dont le h2 contient keyword."""
    blocs = driver.find_elements(By.CSS_SELECTOR, "div.col-md-12.blc")
    for bloc in blocs:
        try:
            h2 = bloc.find_element(By.TAG_NAME, "h2")
            if keyword.lower() in h2.text.lower():
                texte = bloc.text.replace(h2.text, "").strip()
                items = [
                    li.text.strip()
                    for li in bloc.find_elements(By.TAG_NAME, "li")
                    if li.text.strip()
                ]
                return texte, items
        except:
            continue
    return "", []


# =============================================================================
#  SCRAPING PAGE DÉTAIL
# =============================================================================
def _scrape_detail(url: str, driver) -> dict:
    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "ul.featureInfo"))
        )
        time.sleep(2)

        missions, _ = _get_bloc_by_h2(driver, "Poste")
        profil, competences_brutes = _get_bloc_by_h2(driver, "Profil")
        comp = extraire_et_normaliser(competences_brutes)

        traits = []
        try:
            traits = list({
                t.text.strip()
                for t in driver.find_elements(By.CSS_SELECTOR, "span.tagSkills")
                if t.text.strip()
            })
        except:
            pass

        experience = localisation_detail = niveau_etude = ""
        try:
            for item in driver.find_elements(By.CSS_SELECTOR, "ul.featureInfo li"):
                attr = item.get_attribute("title") or ""
                texte = item.text.strip()
                if "Expérience" in attr:
                    experience = texte
                elif "Région" in attr:
                    localisation_detail = texte
                elif "étude" in attr or "formation" in attr:
                    niveau_etude = texte
        except:
            pass

        type_contrat = teletravail = ""
        try:
            for tag in driver.find_elements(By.CSS_SELECTOR, "span.tagContrat"):
                texte = tag.text.strip()
                if "Télétravail" in texte:
                    teletravail = texte.replace("Télétravail :", "").strip()
                elif texte:
                    type_contrat = texte
        except:
            pass

        date_limite = ""
        try:
            date_limite = driver.find_element(By.CSS_SELECTOR, "span.newjob b").text.strip()
        except:
            pass

        desc_entreprise = ""
        try:
            desc_entreprise = driver.find_element(By.CSS_SELECTOR, "div#recruiterDescription p").text.strip()
        except:
            pass

        print(f"      📋 Missions     : {len(missions)} chars")
        print(f"      👤 Profil       : {len(profil)} chars")
        print(f"      🛠️  Brutes       : {comp['competences_brutes'][:3]}...")
        print(f"      ✅ Normalisées  : {comp['competences_liste']}")

        return {
            "experience":             experience,
            "niveau_etude":           niveau_etude,
            "localisation_detail":    localisation_detail,
            "type_contrat":           type_contrat,
            "teletravail":            teletravail,
            "date_limite":            date_limite,
            "desc_entreprise":        desc_entreprise,
            "missions":               missions[:2000],
            "profil":                 profil[:2000],
            "traits_personnalite":    traits,
            "competences_brutes":     comp["competences_brutes"],
            "competences_normalisees": comp["competences_normalisees"],
            "competences_liste":      comp["competences_liste"],
            "competences_nouvelles":  comp["competences_nouvelles"],
        }

    except Exception as e:
        print(f"      ❌ Erreur _scrape_detail : {e}")
        return {
            "experience": "", "niveau_etude": "", "localisation_detail": "",
            "type_contrat": "", "teletravail": "", "date_limite": "",
            "desc_entreprise": "", "missions": "", "profil": "",
            "traits_personnalite": [], "competences_brutes": [],
            "competences_normalisees": {}, "competences_liste": [],
            "competences_nouvelles": [],
        }


# =============================================================================
#  SCRAPER PRINCIPAL
# =============================================================================
def _scrape_rekrute(keywords: str, max_results: int = 20) -> list:
    print("🚀 SCRAPING REKRUTE")
    driver = _get_driver()
    records = []

    try:
        url = f"https://www.rekrute.com/offres.html?keyword={keywords}"
        print(f"🌐 Chargement : {url}")
        driver.get(url)
        time.sleep(5)

        offres_meta = []
        offers = driver.find_elements(By.XPATH, "//li[contains(@class,'post-id')]")
        print(f"📦 {len(offers)} offres trouvées sur la page liste\n")

        for offer in offers[:max_results]:
            try:
                title_el = offer.find_element(By.CSS_SELECTOR, "a.titreJob")
                title = title_el.text.strip()
                link = title_el.get_attribute("href")
                try:
                    desc_courte = offer.find_element(By.CSS_SELECTOR, "div.info span").text.strip()
                except:
                    desc_courte = ""

                if not is_data_engineering_job(title, desc_courte):
                    print(f"  ⏭️  Ignoré : {title[:55]}")
                    continue

                offres_meta.append({
                    "title": title,
                    "link": link,
                    "desc_courte": desc_courte,
                })
                print(f"  ✔️  Retenu  : {title[:55]}")
            except Exception as e:
                print(f"  ⚠️ Erreur lecture liste : {e}")
                continue

        print(f"\n✅ {len(offres_meta)} offres Data Engineering à visiter\n")

        for i, meta in enumerate(offres_meta):
            print(f"\n  [{i+1}/{len(offres_meta)}] {meta['title'][:60]}")
            print(f"      🔍 {meta['link']}")

            detail = _scrape_detail(meta["link"], driver)

            if not detail["competences_brutes"] and meta["desc_courte"]:
                comp = extraire_et_normaliser([meta["desc_courte"]])
                detail["competences_brutes"] = comp["competences_brutes"]
                detail["competences_normalisees"] = comp["competences_normalisees"]
                detail["competences_liste"] = comp["competences_liste"]
                detail["competences_nouvelles"] = comp["competences_nouvelles"]

            records.append({
                "id":                      str(uuid.uuid4()),
                "source":                  "rekrute",
                "title":                   meta["title"],
                "company":                 detail.get("desc_entreprise", "")[:80],
                "link":                    meta["link"],
                "location":                _extract_location(meta["title"]),
                "description":             meta["desc_courte"],
                "desc_entreprise":         detail["desc_entreprise"],
                "missions":                detail["missions"],
                "profil":                  detail["profil"],
                "traits_personnalite":     detail["traits_personnalite"],
                "competences_brutes":      detail["competences_brutes"],
                "competences_normalisees": detail["competences_normalisees"],
                "competences_liste":       detail["competences_liste"],
                "competences_nouvelles":   detail["competences_nouvelles"],
                "experience":              detail["experience"],
                "niveau_etude":            detail["niveau_etude"],
                "type_contrat":            detail["type_contrat"],
                "teletravail":             detail["teletravail"],
                "date_limite":             detail["date_limite"],
                "scraped_at":              datetime.now().isoformat(),
            })
            time.sleep(2)

    finally:
        driver.quit()

    return records


# =============================================================================
#  PIPELINE MINIO (SANS KAFKA)
# =============================================================================
def _run_rekrute(keywords: str, max_results: int) -> dict:
    """Point d'entrée principal — scrape et sauvegarde uniquement dans MinIO (bronze)."""
    print(f"\n{'='*55}")
    print(f"  _run_rekrute | keywords={keywords} | max={max_results}")
    print(f"{'='*55}\n")

    records = _scrape_rekrute(keywords, max_results)

    if not records:
        print("⚠️  Aucune offre Data Engineering trouvée.")
        return {"scraped": 0, "message": "Aucune offre Data Engineering sur Rekrute"}

    minio = get_minio_client()
    path = save_to_minio(minio, records, source="rekrute")

    print(f"\n✅ {len(records)} offres → MinIO bucket 'raw' path  : {path}")

    return {
        "scraped":    len(records),
        "minio_path": path,
        "records":    records,
    }


# =============================================================================
#  TEST SIMPLE (MinIO uniquement)
# =============================================================================
if __name__ == "__main__":
    result = _run_rekrute("data engineer", max_results=5)
    print(result)



