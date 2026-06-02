# scripts/send_notifications.py
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import requests
import mysql.connector
import json
import os

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = os.getenv("SMTP_USER")          # à définir dans .env ou variables Airflow
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")  # mot de passe application

def send_email(to_email, subject, html_content):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = to_email
    part = MIMEText(html_content, "html")
    msg.attach(part)
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(SMTP_USER, to_email, msg.as_string())

def get_recommendations(preferences):
    try:
        resp = requests.post("http://host.docker.internal:8001/recommend", json=preferences, timeout=10)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"Erreur appel API : {e}")
    return []

def main():
    # Connexion à MySQL (les variables d'environnement doivent être accessibles)
    conn = mysql.connector.connect(
        host="mysql",
        port=3306,
        user=os.getenv("MYSQL_USER", "jobs_user"),
        password=os.getenv("MYSQL_PASSWORD", "jobspassword"),
        database=os.getenv("MYSQL_DATABASE", "jobs_db")
    )
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT id, email, preferences FROM users WHERE notifications_enabled = TRUE")
    users = cursor.fetchall()
    for user in users:
        prefs = json.loads(user["preferences"]) if user["preferences"] else {}
        if not prefs.get("competences"):
            continue
        offres = get_recommendations(prefs)
        if not offres:
            continue
        # Construire le HTML des 5 meilleures offres
        html = f"<h2>Bonjour, voici les offres recommandées pour vous</h2><ul>"
        for offre in offres[:5]:
            html += f"<li><b>{offre['titre']}</b> - {offre['entreprise']}<br><a href='{offre['url']}'>Voir l'offre</a></li>"
        html += "</ul><p>Vous pouvez modifier vos préférences dans l'application.</p>"
        send_email(user["email"], "Nouvelles offres data correspondant à votre profil", html)
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()