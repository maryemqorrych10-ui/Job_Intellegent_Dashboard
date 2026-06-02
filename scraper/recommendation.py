# recommendation.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import mysql.connector
import os
from datetime import datetime

app = FastAPI(title="Job Recommendation API")

# Configuration MySQL (à mettre dans config.py)
MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_USER = os.getenv("MYSQL_USER", "jobs_user")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "jobspassword")
MYSQL_DATABASE = os.getenv("MYSQL_DB", "jobs_db")

class UserProfile(BaseModel):
    competences: List[str]          # ex: ["python", "sql", "spark"]
    ville: Optional[str] = None
    pays: Optional[str] = "Maroc"
    type_contrat: Optional[str] = None
    niveau_experience: Optional[str] = None
    teletravail: Optional[str] = None
    max_results: int = 20

def get_db_connection():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )

@app.post("/recommend")
def recommend_offers(profile: UserProfile):
    """
    Recommande des offres d'emploi en fonction du profil utilisateur.
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    # Construction de la requête
    # On récupère les offres qui correspondent aux critères (ville, contrat, etc.)
    # On calcule un score de correspondance des compétences.

    # Étape 1 : obtenir la liste des offres qui matchent au moins une compétence utilisateur
    # (ou on peut aussi prendre toutes les offres et filtrer après)
    # Pour chaque offre, on compte le nombre de compétences en commun.

    # Requête utilisant la table de liaison fact_offre_competence
    # Note : nous devons filtrer sur les compétences fournies par l'utilisateur.

    placeholders = ','.join(['%s'] * len(profile.competences))
    if not profile.competences:
        raise HTTPException(status_code=400, detail="Au moins une compétence requise")

    # Sous-requête pour obtenir pour chaque offre le nombre de compétences correspondant
    # et la liste des compétences (pour affichage)
    sql = f"""
    SELECT 
        o.offre_id,
        d.titre,
        d.entreprise,
        d.url,
        d.description,
        l.ville,
        l.pays,
        l.teletravail,
        c.type_contrat,
        c.niveau_experience,
        (SELECT COUNT(*) FROM fact_offre_competence fc2 
         JOIN dim_competence dc2 ON fc2.competence_id = dc2.competence_id
         WHERE fc2.offre_id = o.offre_id 
           AND dc2.nom IN ({placeholders})) AS matching_competences,
        (SELECT JSON_ARRAYAGG(dc3.nom) FROM fact_offre_competence fc3
         JOIN dim_competence dc3 ON fc3.competence_id = dc3.competence_id
         WHERE fc3.offre_id = o.offre_id) AS all_competences
    FROM fact_offres o
    JOIN dim_offre_detail d ON o.offre_id = d.offre_id
    JOIN dim_localisation l ON o.localisation_id = l.localisation_id
    JOIN dim_contrat c ON o.contrat_id = c.contrat_id
    WHERE 1=1
    """

    params = list(profile.competences)
    # Filtres optionnels
    if profile.ville:
        sql += " AND l.ville = %s"
        params.append(profile.ville)
    if profile.pays:
        sql += " AND l.pays = %s"
        params.append(profile.pays)
    if profile.type_contrat:
        sql += " AND c.type_contrat = %s"
        params.append(profile.type_contrat)
    if profile.niveau_experience:
        sql += " AND c.niveau_experience = %s"
        params.append(profile.niveau_experience)
    if profile.teletravail:
        sql += " AND l.teletravail = %s"
        params.append(profile.teletravail)

    # Ne garder que les offres ayant au moins une compétence en commun
    sql += f" HAVING matching_competences > 0 ORDER BY matching_competences DESC LIMIT {profile.max_results}"

    cursor.execute(sql, params)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    # Calcul du score final (on peut pondérer plus finement)
    recommendations = []
    for row in rows:
        # Score = nombre de compétences en commun * 10
        # + bonus géographique (si ville exacte)
        # + bonus contrat + expérience si correspond
        score = row['matching_competences'] * 10
        if profile.ville and row['ville'] == profile.ville:
            score += 5
        if profile.type_contrat and row['type_contrat'] == profile.type_contrat:
            score += 5
        if profile.niveau_experience and row['niveau_experience'] == profile.niveau_experience:
            score += 5
        recommendations.append({
            "offre_id": row['offre_id'],
            "titre": row['titre'],
            "entreprise": row['entreprise'],
            "url": row['url'],
            "description": row['description'][:300],
            "ville": row['ville'],
            "teletravail": row['teletravail'],
            "type_contrat": row['type_contrat'],
            "niveau_experience": row['niveau_experience'],
            "competences_requises": row['all_competences'] if row['all_competences'] else [],
            "score": score,
            "matching_competences": row['matching_competences']
        })

    return {
        "user_profile": profile.dict(),
        "total_recommendations": len(recommendations),
        "recommendations": sorted(recommendations, key=lambda x: x['score'], reverse=True)
    }

@app.get("/health")
def health():
    return {"status": "recommendation API ready"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)  # port différent de l'API scraper