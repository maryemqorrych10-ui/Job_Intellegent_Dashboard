# scripts/load_star_schema.py
from sqlalchemy import create_engine, text
from datetime import datetime
from config import MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE

def get_engine():
    return create_engine(f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}")

def get_or_create_source(conn, nom):
    conn.execute(text("""
        INSERT IGNORE INTO dim_source (nom, url_base, pays, type_source)
        VALUES (:nom, '', 'Maroc', 'jobboard')
    """), {"nom": nom})
    return conn.execute(text("SELECT source_id FROM dim_source WHERE nom = :nom"), {"nom": nom}).fetchone()[0]

def get_or_create_date(conn, date_str):
    """Parse la date en acceptant plusieurs formats."""
    if not date_str:
        d = datetime.now()
    else:
        date_str_clean = date_str[:10]
        d = None
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y"):
            try:
                d = datetime.strptime(date_str_clean, fmt)
                break
            except:
                continue
        if d is None:
            d = datetime.now()
    conn.execute(text("""
        INSERT IGNORE INTO dim_date (date_complete, jour, mois, mois_nom, trimestre, annee, semaine, jour_semaine)
        VALUES (:dc, :j, :m, :mn, :t, :a, :s, :js)
    """), {
        "dc": d.strftime("%Y-%m-%d"),
        "j": d.day,
        "m": d.month,
        "mn": ["","Janvier","Février","Mars","Avril","Mai","Juin","Juillet","Août","Septembre","Octobre","Novembre","Décembre"][d.month],
        "t": (d.month-1)//3+1,
        "a": d.year,
        "s": d.isocalendar()[1],
        "js": ["Lundi","Mardi","Mercredi","Jeudi","Vendredi","Samedi","Dimanche"][d.weekday()]
    })
    row = conn.execute(text("SELECT date_id FROM dim_date WHERE date_complete = :dc"), {"dc": d.strftime("%Y-%m-%d")}).fetchone()
    return row[0]

def get_or_create_localisation(conn, ville, teletravail):
    ville = ville or "Non précisé"
    teletravail = teletravail or "Non"
    conn.execute(text("""
        INSERT IGNORE INTO dim_localisation (ville, pays, teletravail)
        VALUES (:v, 'Maroc', :t)
    """), {"v": ville, "t": teletravail})
    return conn.execute(text("SELECT localisation_id FROM dim_localisation WHERE ville=:v AND teletravail=:t"), {"v": ville, "t": teletravail}).fetchone()[0]

def get_or_create_contrat(conn, type_contrat, niveau_exp, niveau_etude):
    type_contrat = type_contrat or "Non précisé"
    niveau_exp = niveau_exp or ""
    niveau_etude = niveau_etude or ""
    conn.execute(text("""
        INSERT IGNORE INTO dim_contrat (type_contrat, niveau_experience, niveau_etude)
        VALUES (:tc, :exp, :et)
    """), {"tc": type_contrat, "exp": niveau_exp, "et": niveau_etude})
    return conn.execute(text("SELECT contrat_id FROM dim_contrat WHERE type_contrat=:tc AND niveau_experience=:exp"), {"tc": type_contrat, "exp": niveau_exp}).fetchone()[0]

def get_or_create_competence(conn, nom, categorie):
    nom = nom.lower().strip()
    categorie = categorie or "autre"
    conn.execute(text("INSERT IGNORE INTO dim_competence (nom, categorie) VALUES (:nom, :cat)"), {"nom": nom, "cat": categorie})
    return conn.execute(text("SELECT competence_id FROM dim_competence WHERE nom=:nom"), {"nom": nom}).fetchone()[0]

def charger_offre(conn, offre):
    if conn.execute(text("SELECT 1 FROM dim_offre_detail WHERE hash_offre = :h"), {"h": offre["id"]}).fetchone():
        return None
    source_id = get_or_create_source(conn, offre["source"])
    date_scrape_id = get_or_create_date(conn, offre["scraped_at"])
    date_limite_id = get_or_create_date(conn, offre.get("date_limite"))
    loc_id = get_or_create_localisation(conn, offre["location"], offre["teletravail"])
    contrat_id = get_or_create_contrat(conn, offre["type_contrat"], offre["niveau_experience"], offre["niveau_etude"])
    res = conn.execute(text("""
        INSERT INTO fact_offres (source_id, date_scraped_id, date_publie_id, localisation_id, contrat_id, nb_competences)
        VALUES (:sid, :dsc, :dpb, :lid, :cid, :nbc)
    """), {
        "sid": source_id,
        "dsc": date_scrape_id,
        "dpb": date_limite_id,
        "lid": loc_id,
        "cid": contrat_id,
        "nbc": len(offre["competences_liste"])
    })
    offre_id = res.lastrowid
    conn.execute(text("""
        INSERT INTO dim_offre_detail (offre_id, titre, entreprise, description, missions, profil, url, hash_offre)
        VALUES (:oid, :t, :e, :d, :m, :p, :u, :h)
    """), {
        "oid": offre_id,
        "t": offre["title"][:250],
        "e": offre["company"][:150],
        "d": offre["description"][:2000],
        "m": offre["missions"][:3000],
        "p": offre["profil"][:3000],
        "u": offre["link"][:500],
        "h": offre["id"]
    })
    for comp in offre["competences_liste"]:
        categorie = "autre"
        for cat, lst in offre["competences_normalisees"].items():
            if comp in lst:
                categorie = cat
                break
        comp_id = get_or_create_competence(conn, comp, categorie)
        conn.execute(text("INSERT IGNORE INTO fact_offre_competence (offre_id, competence_id) VALUES (:oid, :cid)"), {"oid": offre_id, "cid": comp_id})
    return offre_id

def load_offres_to_star_schema(offres):
    engine = get_engine()
    inserted = 0
    erreurs = 0
    with engine.begin() as conn:
        for offre in offres:
            try:
                if charger_offre(conn, offre):
                    inserted += 1
            except Exception as e:
                print(f"[Load] Erreur offre {offre.get('id','?')} : {e}")
                erreurs += 1
    print(f"✅ {inserted} offres chargées dans MySQL")
    return {"charge": inserted, "doublons": 0, "erreurs": erreurs}