from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
import requests
import json

app = Flask(__name__)
app.secret_key = 'votre_cle_secrete_tres_longue'

API_BASE = "http://localhost:8001"

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

class User(UserMixin):
    def __init__(self, id, email, token):
        self.id = id
        self.email = email
        self.token = token

@login_manager.user_loader
def load_user(user_id):
    if 'user_email' in session and 'token' in session:
        return User(user_id, session['user_email'], session['token'])
    return None

@app.route('/')
def home():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))
    return render_template('landing.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = request.form['email']
        password = request.form['password']
        try:
            resp = requests.post(f"{API_BASE}/login", json={"email": email, "password": password})
            if resp.status_code == 200:
                data = resp.json()
                user = User(id=email, email=email, token=data['access_token'])
                login_user(user)
                session['token'] = data['access_token']
                session['user_email'] = email
                return redirect(url_for('dashboard'))
            else:
                flash("Identifiants incorrects")
        except Exception as e:
            flash(f"Erreur de connexion : {e}")
    return render_template('login.html')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        email = request.form['email']
        pwd = request.form['password']
        confirm = request.form['confirm']
        if pwd != confirm:
            flash("Mots de passe différents")
        elif len(pwd) < 6:
            flash("Mot de passe trop court (≥6)")
        else:
            try:
                resp = requests.post(f"{API_BASE}/register", json={"email": email, "password": pwd})
                if resp.status_code == 200:
                    flash("Compte créé, vous pouvez vous connecter")
                    return redirect(url_for('login'))
                else:
                    flash("Email déjà utilisé")
            except Exception as e:
                flash(f"Erreur : {e}")
    return render_template('register.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    session.clear()
    return redirect(url_for('home'))

@app.route('/dashboard')
@login_required
def dashboard():
    headers = {"token": session['token']}
    try:
        resp = requests.get(f"{API_BASE}/user/preferences", headers=headers)
        if resp.status_code == 200:
            data = resp.json()
            prefs = data.get('preferences', {})
            notif_enabled = data.get('notifications_enabled', False)
        else:
            prefs = {}
            notif_enabled = False
    except:
        prefs = {}
        notif_enabled = False
    return render_template('dashboard.html', prefs=prefs, notif_enabled=notif_enabled)

@app.route('/update_preferences', methods=['POST'])
@login_required
def update_preferences():
    competences_str = request.form.get('competences', '')
    ville = request.form.get('ville')
    teletravail = 'teletravail' in request.form
    type_contrat = request.form.get('type_contrat')
    niveau_exp = request.form.get('niveau_exp')
    notifications = 'notifications' in request.form
    prefs = {
        "competences": [c.strip() for c in competences_str.split(',') if c.strip()],
        "ville": ville or None,
        "teletravail": teletravail,
        "type_contrat": type_contrat or None,
        "niveau_experience": niveau_exp or None,
        "notifications_enabled": notifications
    }
    headers = {"token": session['token'], "Content-Type": "application/json"}
    try:
        resp = requests.put(f"{API_BASE}/user/preferences", json=prefs, headers=headers)
        if resp.status_code == 200:
            flash("Préférences mises à jour", "success")
        else:
            flash("Erreur lors de la mise à jour", "danger")
    except Exception as e:
        flash(f"Erreur : {e}", "danger")
    return redirect(url_for('dashboard'))

@app.route('/recommend', methods=['POST'])
@login_required
def recommend():
    competences_str = request.form.get('competences', '')
    ville = request.form.get('ville')
    teletravail = 'teletravail' in request.form
    type_contrat = request.form.get('type_contrat')
    niveau_exp = request.form.get('niveau_exp')
    payload = {
        "competences": [c.strip() for c in competences_str.split(',') if c.strip()],
        "ville": ville or None,
        "teletravail": teletravail,
        "type_contrat": type_contrat or None,
        "niveau_experience": niveau_exp or None,
        "notifications_enabled": False
    }
    headers = {"token": session['token']}
    try:
        resp = requests.post(f"{API_BASE}/recommend", json=payload, headers=headers)
        if resp.status_code == 200:
            offres = resp.json()
            return render_template('results.html', offres=offres)
        else:
            flash("Erreur lors de la recherche", "danger")
            return redirect(url_for('dashboard'))
    except Exception as e:
        flash(f"Erreur : {e}", "danger")
        return redirect(url_for('dashboard'))

@app.route('/upload_cv', methods=['POST'])
@login_required
def upload_cv():
    if 'cv' not in request.files:
        flash("Aucun fichier sélectionné", "danger")
        return redirect(url_for('dashboard'))
    file = request.files['cv']
    if file.filename == '':
        flash("Aucun fichier sélectionné", "danger")
        return redirect(url_for('dashboard'))
    ville = request.form.get('ville')
    teletravail = 'teletravail' in request.form
    type_contrat = request.form.get('type_contrat')
    niveau_exp = request.form.get('niveau_exp')
    files = {'file': (file.filename, file.stream, file.mimetype)}
    data = {
        'ville': ville or '',
        'teletravail': str(teletravail).lower(),
        'type_contrat': type_contrat or '',
        'niveau_experience': niveau_exp or ''
    }
    headers = {"token": session['token']}
    try:
        resp = requests.post(f"{API_BASE}/recommend_from_cv", files=files, data=data, headers=headers)
        if resp.status_code == 200:
            offres = resp.json()
            return render_template('results.html', offres=offres)
        else:
            flash("Erreur lors de l'analyse du CV", "danger")
            return redirect(url_for('dashboard'))
    except Exception as e:
        flash(f"Erreur : {e}", "danger")
        return redirect(url_for('dashboard'))

@app.route('/user/history')
@login_required
def user_history():
    headers = {"token": session['token']}
    resp = requests.get(f"{API_BASE}/user/history", headers=headers)
    if resp.status_code == 200:
        return jsonify(resp.json())
    else:
        return jsonify([])

@app.route('/stats/offers_per_day')
def offers_per_day():
    resp = requests.get(f"{API_BASE}/stats/offers_per_day")
    if resp.status_code == 200:
        return jsonify(resp.json())
    else:
        return jsonify([])

if __name__ == '__main__':
    app.run(debug=True, port=5000)