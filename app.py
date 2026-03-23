from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import json, os, math, requests, datetime, re, secrets, random
import psycopg2
from psycopg2.extras import RealDictCursor
from werkzeug.security import generate_password_hash, check_password_hash

# ── Chargement du fichier .env ─────────────────────────────────────────────────
# Charge le .env situé dans le même dossier que app.py, quel que soit
# le répertoire de travail courant (évite le KeyError: 'DB_HOST').
_APP_DIR = os.path.dirname(os.path.abspath(__file__))
_ENV_PATH = os.path.join(_APP_DIR, ".env")

try:
    from dotenv import load_dotenv
    if os.path.exists(_ENV_PATH):
        load_dotenv(_ENV_PATH, override=True)
        print(f"[Config] .env chargé depuis {_ENV_PATH}")
    else:
        print(f"[Config] ⚠️  Fichier .env non trouvé : {_ENV_PATH}")
        print("[Config] Créez le fichier .env dans le dossier de app.py")
except ImportError:
    print("[Config] python-dotenv non installé — pip install python-dotenv")
    print("[Config] Sans python-dotenv, les variables doivent être définies manuellement.")
    # Fallback : si python-dotenv absent, lire le .env manuellement
    if os.path.exists(_ENV_PATH):
        with open(_ENV_PATH, encoding="utf-8") as _f:
            for _line in _f:
                _line = _line.strip()
                if _line and not _line.startswith("#") and "=" in _line:
                    _k, _, _v = _line.partition("=")
                    os.environ.setdefault(_k.strip(), _v.strip())
        print(f"[Config] .env chargé manuellement depuis {_ENV_PATH}")

# ── SendGrid ───────────────────────────────────────────────────────────────────
try:
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail, Content, MimeType
    _SENDGRID_AVAILABLE = True
except ImportError:
    _SENDGRID_AVAILABLE = False
    print("[Email] sendgrid non installé — pip install sendgrid")

app = Flask(__name__)
CORS(app, supports_credentials=True)

# ============================================================
# CONFIG — toutes les clés viennent du fichier .env
# (jamais de secrets en dur dans ce fichier)
# ============================================================
DB_CONFIG = {
    "host":     os.environ["DB_HOST"],
    "database": os.environ["DB_NAME"],
    "user":     os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"],
    "port":     os.environ.get("DB_PORT", "5432"),
}

API_IGN_URL    = "https://apicarto.ign.fr/api/rpg/v2"
# MF_API_KEY n'est plus utilisé directement par app.py
# (l'appel API MF est fait par update_vigilance.py via GitHub Actions)
# On le garde en optional pour ne pas casser un éventuel appel futur.
MF_API_KEY     = os.environ.get("MF_API_KEY", "")
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# ── Chemins des fichiers de données ──────────────────────────────────────────
# En local  : dossier EnvIntel_Agri ou Parisk selon l'installation.
# Sur Render : variable d'environnement DATA_DIR ou dossier persistant /data.
def _find_base_dir():
    if os.environ.get('DATA_DIR') and os.path.isdir(os.environ['DATA_DIR']):
        return os.environ['DATA_DIR']
    candidates = [
        r"C:\Users\godfr\Documents\Parisk",
        r"C:\Users\godfr\Documents\EnvIntel_Agri",
        "/data",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"),
        os.path.dirname(os.path.abspath(__file__)),
    ]
    for d in candidates:
        if os.path.isdir(d):
            return d
    return os.path.dirname(os.path.abspath(__file__))

BASE_DIR           = _find_base_dir()
TRI_GEOJSON_PATH   = os.path.join(BASE_DIR, "n_tri_s.geojson")
MVT_GEOJSON_PATH   = os.path.join(BASE_DIR, "mvt_national.geojson")
DEPTS_GEOJSON_PATH = os.path.join(BASE_DIR, "departements.geojson")
VIGILANCE_OUTPUT   = os.path.join(BASE_DIR, "vigilance_active.geojson")
print(f"[Config] BASE_DIR = {BASE_DIR}")

FREE_PLAN_LIMIT = 100

# ── SendGrid ──────────────────────────────────────────────────────────────────
# Variables d'environnement (configurées dans Render Dashboard → Environment) :
#   SENDGRID_API_KEY  → clé API SendGrid (Settings → API Keys)
#   EMAIL_FROM_ADDR   → adresse vérifiée dans SendGrid (Single Sender Verification)
#   EMAIL_FROM_NAME   → nom affiché dans la boîte du destinataire (ex: "Parisk")
#                       Peut être n'importe quel nom d'application, pas forcément l'email.
# En local : les valeurs ci-dessous sont utilisées si les variables d'env ne sont pas définies.
SENDGRID_API_KEY = os.environ.get('SENDGRID_API_KEY', 'SG.xxxxxxxxxxxxxxxxxx')
EMAIL_FROM_NAME  = os.environ.get('EMAIL_FROM_NAME',  'Parisk')
EMAIL_FROM_ADDR  = os.environ.get('EMAIL_FROM_ADDR',  'votre.email@gmail.com')

# URL de base → utilisée pour le lien de reset password dans l'email
# En production sur Netlify : https://ton-site.netlify.app
APP_BASE_URL = os.environ.get('APP_BASE_URL', 'http://localhost:5000')

# ── Caches mémoire ────────────────────────────────────────────
_pending_signups: dict = {}  # { email: {code, password_hash, expires_at, attempts} }
_reset_tokens:    dict = {}  # { token: {user_id, email, expires_at} }
_tri_cache        = None
_mvt_cache        = None
_vigilance_cache  = None
_sessions:        dict = {}

# ============================================================
# FRANCE BBOX
# ============================================================
FRANCE_BBOX = {
    "min_lon": -5.5, "max_lon": 10.0,
    "min_lat": 41.0, "max_lat": 51.5
}

def is_in_france(lat, lon):
    return (FRANCE_BBOX["min_lat"] <= lat <= FRANCE_BBOX["max_lat"] and
            FRANCE_BBOX["min_lon"] <= lon <= FRANCE_BBOX["max_lon"])

def get_centroid_from_geometry(geometry):
    coords = []
    gtype = geometry.get("type", "")
    raw = geometry.get("coordinates", [])
    if gtype == "Polygon":
        for ring in raw: coords.extend(ring)
    elif gtype == "MultiPolygon":
        for poly in raw:
            for ring in poly: coords.extend(ring)
    if not coords: return None, None
    lon = sum(c[0] for c in coords) / len(coords)
    lat = sum(c[1] for c in coords) / len(coords)
    return lat, lon

# ============================================================
# DB HELPERS
# ============================================================
def get_db():
    return psycopg2.connect(**DB_CONFIG)

def ensure_tables():
    stmts = [
        """CREATE TABLE IF NOT EXISTS users_profiles (
            id             SERIAL PRIMARY KEY,
            email          TEXT UNIQUE NOT NULL,
            password_hash  TEXT NOT NULL,
            selected_zones JSONB DEFAULT '[]'
        );""",
        "ALTER TABLE users_profiles ADD COLUMN IF NOT EXISTS plan TEXT NOT NULL DEFAULT 'free';",
        "ALTER TABLE users_profiles ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();",
        # Préférences utilisateur : langue, unité de mesure
        "ALTER TABLE users_profiles ADD COLUMN IF NOT EXISTS preferences JSONB DEFAULT '{\"lang\":\"fr\",\"unit\":\"metric\"}';",
    ]
    conn = None
    try:
        conn = get_db(); cur = conn.cursor()
        for sql in stmts:
            try:
                cur.execute(sql); conn.commit()
            except Exception as inner:
                conn.rollback(); print(f"[DB] warning: {inner}")
        cur.close(); print("[DB] Tables OK")
    except Exception as e:
        print(f"[DB] ensure_tables error: {e}")
    finally:
        if conn: conn.close()

def validate_password(pw: str):
    if len(pw) < 8:
        return False, "Le mot de passe doit contenir au moins 8 caractères."
    if not re.search(r'[!@#$%^&*()_+\-=\[\]{};\':"\\|,.<>/?`~]', pw):
        return False, "Au moins un caractère spécial requis."
    return True, "OK"

def generate_verification_code() -> str:
    """Génère un code de vérification à 6 chiffres."""
    return f"{random.randint(0, 999999):06d}"

def _send_email(to_email: str, subject: str, text_body: str, html_body: str,
                category: str = "transactional") -> bool:
    """
    Envoi d'email via l'API SendGrid avec headers anti-spam.
    - List-Unsubscribe : requis par Gmail/Yahoo pour les expéditeurs
    - Reply-To : pointe vers l'adresse réelle pour éviter le filtre spam
    - category : tag SendGrid pour les statistiques (transactional | verification | reset)
    Nécessite : pip install sendgrid
    """
    if not _SENDGRID_AVAILABLE:
        print("[Email] sendgrid non installé — pip install sendgrid")
        return False
    if SENDGRID_API_KEY.startswith('SG.xxxxx'):
        print("[Email] SENDGRID_API_KEY non configuré")
        return False
    try:
        from sendgrid.helpers.mail import (
            Mail, Content, MimeType, ReplyTo,
            Header, Category
        )
        message = Mail(
            from_email=(EMAIL_FROM_ADDR, EMAIL_FROM_NAME),
            to_emails=to_email,
            subject=subject,
        )
        message.content = [
            Content(MimeType.text, text_body),
            Content(MimeType.html, html_body),
        ]
        # Anti-spam : Reply-To explicite
        message.reply_to = ReplyTo(EMAIL_FROM_ADDR, EMAIL_FROM_NAME)
        # List-Unsubscribe (exigé par Gmail & Yahoo pour les envois en masse)
        message.header = [
            Header('List-Unsubscribe',
                   f'<mailto:{EMAIL_FROM_ADDR}?subject=unsubscribe>'),
            Header('X-Entity-Ref-ID', secrets.token_hex(8)),
        ]
        # Catégorie SendGrid pour les stats
        message.category = [Category(category)]

        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        status = response.status_code
        if status in (200, 201, 202):
            print(f"[Email] Envoyé à {to_email} (HTTP {status}, cat={category})")
            return True
        print(f"[Email] SendGrid HTTP {status} pour {to_email}: {response.body}")
        return False
    except Exception as e:
        print(f"[Email] Erreur SendGrid vers {to_email}: {e}")
        return False


def send_verification_email(email: str, code: str) -> bool:
    """Envoie le code de vérification 6 chiffres — subject sans emoji pour éviter le spam."""
    # Pas d'emoji dans le subject : les filtres anti-spam les pénalisent
    subject = f"Votre code de verification Parisk : {code}"

    text_body = f"""Bonjour,

Votre code de verification Parisk est :

  {code}

Ce code est valable 10 minutes.

Si vous n'avez pas demande la creation d'un compte, ignorez cet email.

-- L'equipe Parisk""".strip()

    html_body = f"""<!DOCTYPE html>
<html lang="fr">
<head><meta charset="UTF-8"></head>
<body style="font-family:Arial,sans-serif;background:#f4f6f8;margin:0;padding:30px 0;">
  <div style="max-width:420px;margin:0 auto;background:#ffffff;border-radius:12px;
              overflow:hidden;border:1px solid #e2e8f0;">
    <div style="background:#27ae60;padding:24px 30px;">
      <h1 style="color:#ffffff;margin:0;font-size:20px;font-weight:600;">Parisk</h1>
      <p style="color:rgba(255,255,255,0.85);margin:6px 0 0;font-size:14px;">
        Verification de votre adresse email
      </p>
    </div>
    <div style="padding:32px 30px;">
      <p style="color:#374151;font-size:15px;margin:0 0 24px;line-height:1.6;">
        Bonjour,<br><br>
        Voici votre code de verification pour creer votre compte Parisk :
      </p>
      <div style="text-align:center;margin:0 0 28px;">
        <div style="display:inline-block;background:#f0fdf4;border:2px solid #27ae60;
                    border-radius:10px;padding:16px 40px;">
          <span style="font-size:36px;font-weight:700;letter-spacing:12px;
                       color:#15803d;font-family:'Courier New',monospace;">{code}</span>
        </div>
      </div>
      <p style="color:#6b7280;font-size:13px;margin:0 0 8px;">
        Ce code expire dans <strong>10 minutes</strong>.
      </p>
      <p style="color:#6b7280;font-size:13px;margin:0;">
        Si vous n'avez pas demande la creation d'un compte, ignorez cet email.
      </p>
    </div>
    <div style="background:#f8fafc;padding:16px 30px;border-top:1px solid #e2e8f0;">
      <p style="color:#9ca3af;font-size:11px;margin:0;text-align:center;">
        Parisk &middot; France &middot; Ne pas repondre a cet email
      </p>
    </div>
  </div>
</body>
</html>"""

    return _send_email(email, subject, text_body, html_body, category="verification")


def send_reset_email(email: str, reset_link: str) -> bool:
    """Envoie le lien de réinitialisation de mot de passe."""
    subject = "Reinitialisation de votre mot de passe Parisk"

    text_body = f"""Bonjour,

Vous avez demande la reinitialisation de votre mot de passe Parisk.

Cliquez sur ce lien pour choisir un nouveau mot de passe :
{reset_link}

Ce lien est valable 30 minutes et ne peut etre utilise qu'une seule fois.

Si vous n'avez pas fait cette demande, ignorez cet email.

-- L'equipe Parisk""".strip()

    html_body = f"""<!DOCTYPE html>
<html lang="fr">
<head><meta charset="UTF-8"></head>
<body style="font-family:Arial,sans-serif;background:#f4f6f8;margin:0;padding:30px 0;">
  <div style="max-width:420px;margin:0 auto;background:#ffffff;border-radius:12px;
              overflow:hidden;border:1px solid #e2e8f0;">
    <div style="background:#1d4ed8;padding:24px 30px;">
      <h1 style="color:#ffffff;margin:0;font-size:20px;font-weight:600;">Parisk</h1>
      <p style="color:rgba(255,255,255,0.85);margin:6px 0 0;font-size:14px;">
        Reinitialisation de mot de passe
      </p>
    </div>
    <div style="padding:32px 30px;">
      <p style="color:#374151;font-size:15px;margin:0 0 24px;line-height:1.6;">
        Bonjour,<br><br>
        Vous avez demande la reinitialisation de votre mot de passe.<br>
        Cliquez sur le bouton ci-dessous pour en choisir un nouveau :
      </p>
      <div style="text-align:center;margin:0 0 28px;">
        <a href="{reset_link}"
           style="display:inline-block;background:#1d4ed8;color:#ffffff;text-decoration:none;
                  padding:14px 32px;border-radius:25px;font-weight:600;font-size:15px;">
          Reinitialiser mon mot de passe
        </a>
      </div>
      <p style="color:#6b7280;font-size:13px;margin:0 0 12px;">
        Lien valable <strong>30 minutes</strong>, usage unique.
      </p>
      <p style="color:#6b7280;font-size:12px;margin:0 0 8px;">
        Si le bouton ne fonctionne pas, copiez ce lien dans votre navigateur :
      </p>
      <p style="background:#f3f4f6;border-radius:6px;padding:10px 12px;
                font-size:11px;color:#374151;font-family:'Courier New',monospace;
                word-break:break-all;margin:0 0 16px;">{reset_link}</p>
      <p style="color:#9ca3af;font-size:12px;margin:0;">
        Si vous n'avez pas fait cette demande, ignorez cet email.
      </p>
    </div>
    <div style="background:#f8fafc;padding:16px 30px;border-top:1px solid #e2e8f0;">
      <p style="color:#9ca3af;font-size:11px;margin:0;text-align:center;">
        Parisk &middot; France &middot; Ne pas repondre a cet email
      </p>
    </div>
  </div>
</body>
</html>"""

    return _send_email(email, subject, text_body, html_body, category="reset")

def create_session(user_id: int) -> str:
    token = secrets.token_hex(32)
    _sessions[token] = user_id
    return token

def get_user_from_token(token: str):
    if not token: return None
    user_id = _sessions.get(token)
    if not user_id: return None
    conn = None
    try:
        conn = get_db(); cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT id, email, selected_zones FROM users_profiles WHERE id = %s", (user_id,))
        return cur.fetchone()
    except Exception: return None
    finally:
        if conn: conn.close()

def require_auth(f):
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        auth  = request.headers.get('Authorization', '')
        token = auth.replace('Bearer ', '').strip() if auth.startswith('Bearer ') else ''
        user  = get_user_from_token(token)
        if not user:
            return jsonify({"error": "Authentification requise."}), 401
        request.current_user = user
        return f(*args, **kwargs)
    return decorated

def get_plan(user_id):
    conn = None
    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute("SELECT plan FROM users_profiles WHERE id = %s", (user_id,))
        row = cur.fetchone()
        return row[0] if row else 'free'
    except Exception: return 'free'
    finally:
        if conn: conn.close()

# ============================================================
# IGN HELPERS
# ============================================================
def generate_circle_polygon(lat, lng, radius_km, n_points=32):
    coords = []; R_earth = 6371.0
    for i in range(n_points):
        angle = 2 * math.pi * i / n_points
        dlat  = (radius_km / R_earth) * math.degrees(1)
        dlng  = dlat / math.cos(math.radians(lat))
        coords.append([lng + dlng * math.cos(angle), lat + dlat * math.sin(angle)])
    coords.append(coords[0])
    return {"type": "Polygon", "coordinates": [coords]}

def call_ign(geometry, annee=2023):
    try:
        params = {"annee": annee, "geom": json.dumps(geometry), "_limit": 200}
        resp   = requests.get(API_IGN_URL, params=params, timeout=20)
        if resp.status_code == 200: return resp.json()
        return None
    except Exception as e:
        print(f"[IGN] Error: {e}"); return None

def enrich_ids(geojson):
    if not geojson or 'features' not in geojson: return geojson
    for i, feat in enumerate(geojson.get('features', [])):
        props = feat.get('properties') or {}
        if not props.get('id'):
            pid = (props.get('id_parcel') or props.get('id_ilot') or props.get('code_cultu') or str(i))
            props['id'] = f"ign_{pid}_{i}"
        feat['properties'] = props
    return geojson

# ============================================================
# OPEN-METEO
# ============================================================
def deg_to_arrow(deg):
    arrows = ["↑", "↗", "→", "↘", "↓", "↙", "←", "↖"]
    return arrows[round(deg / 45) % 8]

def get_wmo_label(code):
    WMO = {
        0:("Ciel dégagé","☀️"), 1:("Peu nuageux","🌤️"), 2:("Partiellement nuageux","⛅"),
        3:("Couvert","☁️"), 45:("Brouillard","🌫️"), 48:("Brouillard givrant","🌫️"),
        51:("Bruine légère","🌦️"), 53:("Bruine modérée","🌦️"), 55:("Bruine dense","🌧️"),
        61:("Pluie légère","🌧️"), 63:("Pluie modérée","🌧️"), 65:("Pluie forte","🌧️"),
        71:("Neige légère","🌨️"), 73:("Neige modérée","❄️"), 75:("Neige forte","❄️"),
        80:("Averses légères","🌦️"), 81:("Averses modérées","🌧️"), 82:("Averses violentes","⛈️"),
        95:("Orage","⛈️"), 96:("Orage avec grêle","⛈️"), 99:("Orage violent","⛈️"),
    }
    return WMO.get(code, ("Inconnu", "❓"))

@app.route('/api/meteo/open-meteo', methods=['GET'])
def get_open_meteo_data():
    lat = request.args.get('lat')
    lon = request.args.get('lon')
    try:
        day_offset = max(0, min(int(request.args.get('day', 0)), 6))
    except ValueError:
        day_offset = 0
    if not lat or not lon:
        return jsonify({"error": "lat et lon obligatoires."}), 400
    try:
        params = {
            "latitude": lat, "longitude": lon, "timezone": "auto", "forecast_days": 7,
            "hourly": "temperature_2m,precipitation,relative_humidity_2m,wind_speed_10m,wind_direction_10m,cloud_cover,dew_point_2m,weather_code,soil_moisture_0_to_1cm,et0_fao_evapotranspiration",
            "daily":  "weather_code,temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max,wind_direction_10m_dominant,relative_humidity_2m_max,relative_humidity_2m_min,dew_point_2m_mean,cloud_cover_mean,et0_fao_evapotranspiration_sum,precipitation_probability_max",
            "current":"temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,wind_direction_10m,weather_code,cloud_cover,dew_point_2m",
        }
        resp = requests.get(OPEN_METEO_URL, params=params, timeout=15)
        if resp.status_code != 200:
            return jsonify({"error": f"Open-Meteo HTTP {resp.status_code}"}), 502
        raw = resp.json()
        cur_r = raw.get("current", {}); h_r = raw.get("hourly", {}); d_r = raw.get("daily", {})
        wl, wi = get_wmo_label(cur_r.get("weather_code", 0))
        current = {
            "temperature": cur_r.get("temperature_2m"), "humidity": cur_r.get("relative_humidity_2m"),
            "precipitation": cur_r.get("precipitation"), "wind_speed": cur_r.get("wind_speed_10m"),
            "wind_dir": cur_r.get("wind_direction_10m"),
            "wind_arrow": deg_to_arrow(cur_r.get("wind_direction_10m") or 0),
            "cloud_cover": cur_r.get("cloud_cover"), "dew_point": cur_r.get("dew_point_2m"),
            "weather_code": cur_r.get("weather_code"), "weather_label": wl, "weather_icon": wi,
            "time": cur_r.get("time"),
        }
        target_date = (datetime.date.today() + datetime.timedelta(days=day_offset)).isoformat()
        times = h_r.get("time", [])
        hourly_day = {"times":[],"temperature":[],"precipitation":[],"humidity":[],"wind_speed":[],"wind_dir":[],"cloud_cover":[],"dew_point":[],"weather_code":[],"soil_moisture":[],"et0":[]}
        def hv(key, i): a=h_r.get(key,[]); return a[i] if i<len(a) else None
        for i, t in enumerate(times):
            if t.startswith(target_date):
                hourly_day["times"].append(t[11:16])
                for k,bk in {"temperature":"temperature_2m","precipitation":"precipitation","humidity":"relative_humidity_2m","wind_speed":"wind_speed_10m","wind_dir":"wind_direction_10m","cloud_cover":"cloud_cover","dew_point":"dew_point_2m","weather_code":"weather_code","soil_moisture":"soil_moisture_0_to_1cm","et0":"et0_fao_evapotranspiration"}.items():
                    hourly_day[k].append(hv(bk, i))
        def dv(key, i): a=d_r.get(key,[]); return a[i] if i<len(a) else None
        daily_dates = d_r.get("time", [])
        day_names_fr = ["Lun.", "Mar.", "Mer.", "Jeu.", "Ven.", "Sam.", "Dim."]
        daily_list = []
        for i, date_str in enumerate(daily_dates):
            code = dv("weather_code", i) or 0; wl2, wi2 = get_wmo_label(code)
            wind_deg = dv("wind_direction_10m_dominant", i)
            try:
                d = datetime.date.fromisoformat(date_str)
                day_name = "Aujourd'hui" if i==0 else day_names_fr[d.weekday()]
                day_fmt  = d.strftime("%d/%m")
            except: day_name = date_str; day_fmt = date_str
            daily_list.append({
                "date": date_str, "day_name": day_name, "day_fmt": day_fmt,
                "weather_code": code, "weather_label": wl2, "weather_icon": wi2,
                "temp_max": dv("temperature_2m_max", i), "temp_min": dv("temperature_2m_min", i),
                "precipitation": dv("precipitation_sum", i), "wind_speed": dv("wind_speed_10m_max", i),
                "wind_dir": wind_deg, "wind_arrow": deg_to_arrow(wind_deg) if wind_deg is not None else "→",
                "humidity_max": dv("relative_humidity_2m_max", i), "humidity_min": dv("relative_humidity_2m_min", i),
                "cloud_cover": dv("cloud_cover_mean", i), "dew_point": dv("dew_point_2m_mean", i),
                "et0": dv("et0_fao_evapotranspiration_sum", i),
                "precip_prob_max": dv("precipitation_probability_max", i),
            })
        return jsonify({"current": current, "hourly": hourly_day, "daily": daily_list,
                        "day_offset": day_offset, "target_date": target_date, "lat": lat, "lon": lon})
    except Exception as e:
        print(f"[Open-Meteo] Error: {e}")
        return jsonify({"error": str(e)}), 500

# ============================================================
# OPEN-METEO HISTORICAL (sécheresse)
# ============================================================
@app.route('/api/meteo/historical', methods=['GET'])
def get_historical_data():
    lat = request.args.get('lat')
    lon = request.args.get('lon')
    if not lat or not lon:
        return jsonify({"error": "lat et lon obligatoires."}), 400
    try:
        end_date = datetime.date.today()
        start_date = end_date - datetime.timedelta(days=90)
        params = {
            "latitude": lat, "longitude": lon,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "daily": "precipitation_sum,et0_fao_evapotranspiration,temperature_2m_max",
            "timezone": "auto",
        }
        resp = requests.get("https://archive-api.open-meteo.com/v1/archive", params=params, timeout=15)
        if resp.status_code != 200:
            return jsonify({"error": f"Archive API HTTP {resp.status_code}"}), 502
        return jsonify(resp.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ============================================================
# GEOMETRY HELPERS
# ============================================================
def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

def point_in_polygon(point, polygon_coords):
    x, y = point
    n = len(polygon_coords)
    inside = False
    j = n - 1
    for i in range(n):
        xi, yi = polygon_coords[i]
        xj, yj = polygon_coords[j]
        if ((yi > y) != (yj > y)) and (x < (xj - xi) * (y - yi) / (yj - yi) + xi):
            inside = not inside
        j = i
    return inside

def point_in_geojson(pt, geom):
    gtype = geom.get("type","")
    coords = geom.get("coordinates",[])
    if gtype == "Polygon":
        return point_in_polygon(pt, coords[0])
    elif gtype == "MultiPolygon":
        return any(point_in_polygon(pt, poly[0]) for poly in coords)
    return False

def flatten_coords(coords):
    if not coords: return []
    if isinstance(coords[0], (int, float)): return [coords]
    result = []
    for c in coords:
        result.extend(flatten_coords(c))
    return result

def dist_to_geometry(lat, lon, geom):
    pts = flatten_coords(geom.get("coordinates", []))
    if not pts: return float('inf')
    return min(haversine(lat, lon, p[1], p[0]) for p in pts)

# ============================================================
# VIGILANCE — cache mémoire (fichier mis à jour par GitHub Actions)
# ============================================================

# Constantes utilisées par /api/vigilance pour servir le cache
VIGI_COLORS = {1: "vert", 2: "jaune", 3: "orange", 4: "rouge"}
VIGI_HEX    = {1: "#1e8449", 2: "#d97706", 3: "#c2410c", 4: "#b91c1c"}


def update_vigilance():
    """
    Charge vigilance_active.geojson en cache mémoire.
    Le fichier est mis à jour toutes les 5h par GitHub Actions (update_vigilance.py).
    Cette fonction est appelée une seule fois au démarrage de l'app.
    """
    global _vigilance_cache
    if not os.path.exists(VIGILANCE_OUTPUT):
        print("[Vigilance] Fichier introuvable — cache non chargé")
        return
    try:
        with open(VIGILANCE_OUTPUT, "r", encoding="utf-8") as f:
            gj = json.load(f)
        alerts = [
            a for feat in gj.get("features", [])
            for a in feat.get("properties", {}).get("vigi_alerts", [])
        ]
        _vigilance_cache = {
            "alerts":     alerts,
            "geojson":    gj,
            "updated_at": gj.get("updated_at"),
        }
        n_alert = sum(1 for feat in gj.get("features", [])
                      if feat.get("properties", {}).get("vigi_level", 1) >= 2)
        print(f"[Vigilance] Cache chargé — {len(alerts)} alertes, "
              f"{n_alert} depts en vigilance, "
              f"updated_at={gj.get('updated_at', '?')}")
    except Exception as e:
        print(f"[Vigilance] Erreur chargement cache: {e}")

# ============================================================
# TRI ANALYSIS
# ============================================================
def load_tri():
    global _tri_cache
    if _tri_cache is not None: return _tri_cache
    if not os.path.exists(TRI_GEOJSON_PATH): return None
    try:
        with open(TRI_GEOJSON_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        for feat in data.get('features', []):
            p = feat.get('properties', {})
            p['nom_tri'] = (p.get('nom_tri') or p.get('NomTRI') or p.get('libelle') or p.get('LIBELLE') or 'TRI')
            feat['properties'] = p
        _tri_cache = data
        return _tri_cache
    except Exception as e:
        print(f"[TRI] load error: {e}"); return None

def compute_tri_risk(geometry):
    tri = load_tri()
    if not tri or not tri.get('features'): return {"risk_level": "safe", "risk_label": "Aucun risque TRI", "nearest_nom": "—", "nearest_dist": 0}
    lat, lon = get_centroid_from_geometry(geometry)
    if lat is None: return {"risk_level": "safe", "risk_label": "—", "nearest_nom": "—", "nearest_dist": 0}
    centroid_pt = [lon, lat]
    best_dist = float('inf'); best_nom = "—"; inside_nom = None
    for feat in tri['features']:
        geom = feat.get('geometry')
        if not geom: continue
        nom = feat.get('properties', {}).get('nom_tri', 'TRI')
        if point_in_geojson(centroid_pt, geom):
            inside_nom = nom; best_dist = 0; best_nom = nom; break
        d = dist_to_geometry(lat, lon, geom)
        if d < best_dist: best_dist = d; best_nom = nom
    best_dist_r = round(best_dist, 2)
    if inside_nom:
        return {"risk_level": "inside", "risk_label": "Dans une zone TRI", "nearest_nom": inside_nom, "nearest_dist": 0}
    elif best_dist < 2:
        return {"risk_level": "close", "risk_label": "Très proche TRI", "nearest_nom": best_nom, "nearest_dist": best_dist_r}
    elif best_dist < 10:
        return {"risk_level": "medium", "risk_label": "Modéré", "nearest_nom": best_nom, "nearest_dist": best_dist_r}
    elif best_dist < 30:
        return {"risk_level": "far", "risk_label": "Éloigné", "nearest_nom": best_nom, "nearest_dist": best_dist_r}
    else:
        return {"risk_level": "safe", "risk_label": "Sécurisé", "nearest_nom": best_nom, "nearest_dist": best_dist_r}

@app.route('/api/tri', methods=['GET'])
def get_tri():
    data = load_tri()
    if not data:
        return jsonify({"type":"FeatureCollection","features":[],"error":"TRI introuvable"}), 404
    return jsonify(data)

@app.route('/api/tri/analyse', methods=['POST'])
def tri_analyse():
    data = request.json or {}
    parcels = data.get('parcels', [])
    results = []
    for p in parcels:
        geometry = p.get('geometry')
        if not geometry:
            results.append({"parcel_id": p.get('id'), "risk_level": "safe", "risk_label": "—"})
            continue
        risk = compute_tri_risk(geometry)
        results.append({"parcel_id": p.get('id'), "parcel_label": p.get('label', ''), **risk})
    return jsonify({"results": results})

# ============================================================
# MVT TERRAIN ANALYSIS
# ============================================================
def load_mvt():
    global _mvt_cache
    if _mvt_cache is not None: return _mvt_cache
    if not os.path.exists(MVT_GEOJSON_PATH): return None
    try:
        with open(MVT_GEOJSON_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        _mvt_cache = data
        return _mvt_cache
    except Exception as e:
        print(f"[MVT] load error: {e}"); return None

def compute_mvt_risk(geometry):
    mvt = load_mvt()
    lat, lon = get_centroid_from_geometry(geometry)
    if lat is None: return {"risk_level": "unknown", "risk_label": "Données insuffisantes", "nearest_mvt": None}
    centroid_pt = [lon, lat]
    if not mvt or not mvt.get('features'):
        return {"risk_level": "unknown", "risk_label": "Données non disponibles", "nearest_mvt": None, "in_risk_zone": False}
    in_zone = False; nearest = None; best_dist = float('inf'); nearby = []
    for feat in mvt['features']:
        geom = feat.get('geometry')
        if not geom: continue
        props = feat.get('properties', {})
        if point_in_geojson(centroid_pt, geom): in_zone = True
        d = dist_to_geometry(lat, lon, geom)
        commune = props.get('commune') or props.get('Commune') or props.get('COMMUNE') or '—'
        type_mvt = props.get('typeMvt') or props.get('type_mvt') or props.get('TYPE_MVT') or 0
        date_debut = props.get('dateDebut') or props.get('date_debut') or ''
        entry = {"commune": commune, "distance_km": round(d, 2), "typeMvt": type_mvt, "dateDebut": date_debut}
        if d < best_dist: best_dist = d; nearest = entry
        if d < 50: nearby.append(entry)
    nearby.sort(key=lambda x: x['distance_km'])
    if in_zone: risk_level = "high"; risk_label = "Zone à risque MVT"
    elif best_dist < 2: risk_level = "high"; risk_label = "Risque élevé"
    elif best_dist < 10: risk_level = "medium"; risk_label = "Risque modéré"
    elif best_dist < 30: risk_level = "low"; risk_label = "Risque faible"
    else: risk_level = "safe"; risk_label = "Risque très faible"
    return {"risk_level": risk_level, "risk_label": risk_label, "nearest_mvt": nearest,
            "nearby_mvt": nearby[:5], "in_risk_zone": in_zone}

@app.route('/api/mvt/analyse', methods=['POST'])
def mvt_analyse():
    data = request.json or {}
    parcels = data.get('parcels', [])
    results = []
    for p in parcels:
        geometry = p.get('geometry')
        if not geometry:
            results.append({"parcel_id": p.get('id'), "risk_level": "unknown", "risk_label": "—"})
            continue
        risk = compute_mvt_risk(geometry)
        results.append({"parcel_id": p.get('id'), "parcel_label": p.get('label', ''), **risk})
    return jsonify({"results": results})

# ============================================================
# SÉCHERESSE (VigiEau + Open-Meteo)
# ============================================================
VIGIEU_URL = "https://api.vigieau.beta.gouv.fr/api/restrictions"

def get_vigieau_data(lat, lon):
    try:
        params = {"latitude": lat, "longitude": lon}
        resp = requests.get(VIGIEU_URL, params=params, timeout=10)
        if resp.status_code == 200: return resp.json()
        return None
    except Exception as e:
        print(f"[VigiEau] Error: {e}"); return None

def compute_secheresse_from_open_meteo(lat, lon):
    try:
        end_date = datetime.date.today()
        start_date = end_date - datetime.timedelta(days=90)
        params = {
            "latitude": lat, "longitude": lon,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "daily": "precipitation_sum,et0_fao_evapotranspiration",
            "timezone": "auto",
        }
        resp = requests.get("https://archive-api.open-meteo.com/v1/archive", params=params, timeout=15)
        if resp.status_code != 200: return None
        raw = resp.json()
        daily = raw.get("daily", {})
        precip = sum(v for v in daily.get("precipitation_sum", []) if v is not None)
        et0 = sum(v for v in daily.get("et0_fao_evapotranspiration", []) if v is not None)
        deficit = precip - et0
        if deficit < -150: level = "crise"
        elif deficit < -80: level = "alerte_renforcee"
        elif deficit < -40: level = "alerte"
        elif deficit < -10: level = "vigilance"
        else: level = "normal"
        colors = {"normal":"#27ae60","vigilance":"#d97706","alerte":"#ea580c","alerte_renforcee":"#dc2626","crise":"#7f1d1d"}
        labels = {"normal":"Normal","vigilance":"Vigilance","alerte":"Alerte","alerte_renforcee":"Alerte renforcée","crise":"Crise"}
        icons = {"normal":"💧","vigilance":"⚠️","alerte":"🚨","alerte_renforcee":"🔴","crise":"⛔"}
        return {
            "max_level": level, "max_level_label": labels.get(level, level),
            "max_level_color": colors.get(level, "#6b7280"),
            "max_level_icon": icons.get(level, "❓"),
            "n_zones": 1, "source": "Open-Meteo",
            "deficit_hydrique": round(deficit, 1),
            "precip_90d": round(precip, 1), "et0_90d": round(et0, 1),
            "usages_agricoles": []
        }
    except Exception as e:
        print(f"[Secheresse OM] Error: {e}"); return None

@app.route('/api/secheresse/analyse', methods=['POST'])
def secheresse_analyse():
    data = request.json or {}
    parcels = data.get('parcels', [])
    results = []
    for p in parcels:
        geometry = p.get('geometry')
        if not geometry:
            results.append({"parcel_id": p.get('id'), "max_level": "normal", "max_level_label": "—"})
            continue
        lat, lon = get_centroid_from_geometry(geometry)
        if lat is None:
            results.append({"parcel_id": p.get('id'), "max_level": "normal"})
            continue
        result = None
        if is_in_france(lat, lon):
            vigi = get_vigieau_data(lat, lon)
            if vigi:
                levels = [r.get('niveauAlerte', 'normal').lower().replace(' ', '_') for r in (vigi if isinstance(vigi, list) else [])]
                level_order = ["normal","vigilance","alerte","alerte_renforcee","crise"]
                max_level = max(levels, key=lambda x: level_order.index(x) if x in level_order else -1) if levels else "normal"
                colors = {"normal":"#27ae60","vigilance":"#d97706","alerte":"#ea580c","alerte_renforcee":"#dc2626","crise":"#7f1d1d"}
                labels = {"normal":"Normal","vigilance":"Vigilance","alerte":"Alerte","alerte_renforcee":"Alerte renforcée","crise":"Crise"}
                icons = {"normal":"💧","vigilance":"⚠️","alerte":"🚨","alerte_renforcee":"🔴","crise":"⛔"}
                usages = []
                for r in (vigi if isinstance(vigi, list) else []):
                    for u in r.get('usages', [])[:3]:
                        usages.append({"thematique": u.get('thematique', ''), "nom": u.get('nom', ''), "description": u.get('description', '')})
                result = {
                    "max_level": max_level, "max_level_label": labels.get(max_level, max_level),
                    "max_level_color": colors.get(max_level, "#6b7280"),
                    "max_level_icon": icons.get(max_level, "❓"),
                    "n_zones": len(vigi) if isinstance(vigi, list) else 0,
                    "source": "VigiEau (France)",
                    "usages_agricoles": usages[:4]
                }
        if result is None:
            result = compute_secheresse_from_open_meteo(lat, lon)
        if result is None:
            result = {"max_level": "normal", "max_level_label": "Normal", "max_level_color": "#27ae60", "max_level_icon": "💧", "n_zones": 0, "source": "Inconnu", "usages_agricoles": []}
        results.append({"parcel_id": p.get('id'), "parcel_label": p.get('label', ''), **result})
    return jsonify({"results": results})

# ============================================================
# INONDATION — TRI + Vigilance + Météo (sans flood API externe)
# ============================================================
@app.route('/api/inondation/analyse', methods=['POST'])
def inondation_analyse():
    """Analyse inondation : TRI local + vigilance météo + prévisions pluie."""
    data = request.json or {}
    parcels = data.get('parcels', [])
    results = []
    for p in parcels:
        geometry = p.get('geometry')
        if not geometry:
            results.append({"parcel_id": p.get('id'), "tri": None})
            continue
        tri_result = compute_tri_risk(geometry)
        lat, lon = get_centroid_from_geometry(geometry)

        # Prévisions précipitations (Open-Meteo forecast simple)
        precip_forecast = None
        if lat is not None:
            try:
                params = {
                    "latitude": lat, "longitude": lon,
                    "daily": "precipitation_sum,precipitation_probability_max",
                    "forecast_days": 7, "timezone": "auto",
                }
                resp = requests.get(OPEN_METEO_URL, params=params, timeout=10)
                if resp.status_code == 200:
                    raw = resp.json()
                    daily = raw.get("daily", {})
                    precips = daily.get("precipitation_sum", [])
                    probs = daily.get("precipitation_probability_max", [])
                    total_7d = sum(v for v in precips if v is not None)
                    max_day = max((v for v in precips if v is not None), default=0)
                    # Niveau de risque pluie
                    if max_day > 40: rain_risk = "high"
                    elif max_day > 20: rain_risk = "medium"
                    elif max_day > 10: rain_risk = "low_medium"
                    else: rain_risk = "low"
                    precip_forecast = {
                        "total_7d": round(total_7d, 1),
                        "max_day": round(max_day, 1),
                        "risk_level": rain_risk,
                        "daily_precip": precips[:7],
                        "daily_prob": probs[:7]
                    }
            except Exception as e:
                print(f"[Inond precip] {e}")

        results.append({
            "parcel_id": p.get('id'), "parcel_label": p.get('label', ''),
            "tri": tri_result,
            "precip_forecast": precip_forecast,
            "is_france": is_in_france(lat, lon) if lat else False
        })
    return jsonify({"results": results})

# ============================================================
# VIGILANCE ROUTES
# ============================================================
@app.route('/api/vigilance', methods=['GET'])
def get_vigilance():
    """Retourne le cache mémoire de vigilance (alertes + geojson enrichi).
    N'appelle JAMAIS update_vigilance() si le cache est déjà chargé —
    la mise à jour est gérée exclusivement par le scheduler (toutes les heures)."""
    if _vigilance_cache:
        return jsonify(_vigilance_cache)
    # Cache vide uniquement au démarrage avant le premier cycle du scheduler
    # → servir le fichier existant sur disque sans appeler l'API MF
    if os.path.exists(VIGILANCE_OUTPUT):
        try:
            with open(VIGILANCE_OUTPUT, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return jsonify({"alerts": [], "geojson": data, "updated_at": data.get("updated_at")})
        except Exception:
            pass
    return jsonify({"alerts":[], "geojson":{"type":"FeatureCollection","features":[]}, "updated_at":None}), 503

@app.route('/api/static/vigilance_active.geojson')
def serve_vigi():
    """Sert le fichier vigilance_active.geojson enrichi.
    Cache navigateur 1 heure (correspond à l'intervalle de mise à jour backend)."""
    if os.path.exists(VIGILANCE_OUTPUT):
        try:
            with open(VIGILANCE_OUTPUT, 'r', encoding='utf-8') as f:
                data = json.load(f)
            resp = jsonify(data)
            resp.headers['Cache-Control'] = 'public, max-age=3600'
            return resp
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    return jsonify({"error":"Fichier non généré"}), 404

# ============================================================
# IGN PARCELLES ROUTE
# ============================================================
@app.route('/api/parcelles', methods=['GET'])
def get_parcelles():
    try:
        lat=float(request.args.get('lat')); lng=float(request.args.get('lng'))
        r_km=float(request.args.get('radius',5))
        if not is_in_france(lat, lng):
            return jsonify({"type":"FeatureCollection","features":[],"not_france":True,"message":"Hors France : utilisez le tracé polygone ou l'import."}), 200
        data=call_ign(generate_circle_polygon(lat,lng,r_km))
        if not data: return jsonify({"type":"FeatureCollection","features":[],"error":"IGN indisponible"}),500
        return jsonify(enrich_ids(data))
    except Exception as e:
        return jsonify({"type":"FeatureCollection","features":[],"error":str(e)}),500

# ============================================================
# GEOCODING
# ============================================================
@app.route('/api/geocode', methods=['GET'])
def geocode():
    q = request.args.get('q', '').strip()
    if not q: return jsonify({"features": []}), 400
    try:
        fr_resp = requests.get(
            f"https://api-adresse.data.gouv.fr/search/?q={requests.utils.quote(q)}&limit=5",
            timeout=5, headers={"User-Agent": "Parisk/1.0"}
        )
        if fr_resp.status_code == 200:
            fr_data = fr_resp.json()
            if fr_data.get('features'): return jsonify(fr_data)
    except Exception: pass
    try:
        nom_resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": q, "format": "geojson", "limit": 6, "addressdetails": 1},
            timeout=8, headers={"User-Agent": "Parisk/1.0 contact@parisk.fr"}
        )
        if nom_resp.status_code == 200:
            data = nom_resp.json()
            features = []
            for feat in data.get('features', []):
                props = feat.get('properties', {}); addr = props.get('address', {})
                label = props.get('display_name', q)
                features.append({
                    "type": "Feature", "geometry": feat['geometry'],
                    "properties": {
                        "label": label, "name": props.get('name', label.split(',')[0]),
                        "postcode": addr.get('postcode', ''),
                        "city": addr.get('city') or addr.get('town') or addr.get('village') or addr.get('county', ''),
                        "country": addr.get('country', ''),
                        "country_code": addr.get('country_code', '').upper(),
                    }
                })
            return jsonify({"type": "FeatureCollection", "features": features})
    except Exception as e:
        print(f"[Geocode] Error: {e}")
    return jsonify({"features": []}), 200

# ============================================================
# AUTH ROUTES
# ============================================================
@app.route('/api/auth/signup', methods=['POST'])
def signup():
    """
    Étape 1 : demande de création de compte.
    Génère un code à 6 chiffres, l'envoie par email, stocke en attente.
    Ne crée PAS encore le compte en base.
    """
    data    = request.json or {}
    email   = (data.get('email') or '').strip().lower()
    password = data.get('password', '')
    if not email or not password:
        return jsonify({"error": "Email et mot de passe requis."}), 400
    ok, msg = validate_password(password)
    if not ok:
        return jsonify({"error": msg}), 400

    conn = None
    try:
        # Vérifier que l'email n'est pas déjà utilisé
        conn = get_db(); cur = conn.cursor()
        cur.execute("SELECT id FROM users_profiles WHERE email = %s", (email,))
        if cur.fetchone():
            return jsonify({"error": "Cet email est déjà utilisé."}), 409
    except Exception:
        return jsonify({"error": "Erreur serveur."}), 500
    finally:
        if conn: conn.close()

    # Nettoyage des anciennes entrées expirées
    now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    expired = [e for e, v in _pending_signups.items() if v["expires_at"] < now]
    for e in expired:
        _pending_signups.pop(e, None)

    # Générer et stocker le code
    code      = generate_verification_code()
    pw_hash   = generate_password_hash(password)
    _pending_signups[email] = {
        "code":          code,
        "password_hash": pw_hash,
        "expires_at":    now + datetime.timedelta(minutes=10),
        "attempts":      0,
    }

    # Envoyer l'email
    sent = send_verification_email(email, code)
    if not sent:
        _pending_signups.pop(email, None)
        return jsonify({"error": "Impossible d'envoyer l'email de vérification. Vérifiez votre adresse."}), 502

    return jsonify({
        "status":  "pending_verification",
        "message": f"Un code de vérification à 6 chiffres a été envoyé à {email}.",
        "email":   email,
    }), 200


@app.route('/api/auth/resend-code', methods=['POST'])
def resend_code():
    """Renvoie un nouveau code si l'inscription est en attente."""
    data  = request.json or {}
    email = (data.get('email') or '').strip().lower()
    if not email:
        return jsonify({"error": "Email requis."}), 400

    pending = _pending_signups.get(email)
    if not pending:
        return jsonify({"error": "Aucune inscription en attente pour cet email."}), 404

    # Générer un nouveau code et remettre l'expiration à 10 minutes
    code = generate_verification_code()
    _pending_signups[email]["code"]       = code
    _pending_signups[email]["expires_at"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) + datetime.timedelta(minutes=10)
    _pending_signups[email]["attempts"]   = 0

    sent = send_verification_email(email, code)
    if not sent:
        return jsonify({"error": "Impossible d'envoyer l'email."}), 502

    return jsonify({"status": "ok", "message": "Nouveau code envoyé."}), 200


@app.route('/api/auth/verify-email', methods=['POST'])
def verify_email():
    """
    Étape 2 : vérification du code.
    Si OK → crée le compte en base + retourne un token de session.
    """
    data  = request.json or {}
    email = (data.get('email') or '').strip().lower()
    code  = str(data.get('code') or '').strip()

    if not email or not code:
        return jsonify({"error": "Email et code requis."}), 400

    pending = _pending_signups.get(email)
    if not pending:
        return jsonify({"error": "Aucune inscription en attente pour cet email. Recommencez."}), 404

    # Vérifier l'expiration
    if datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) > pending["expires_at"]:
        _pending_signups.pop(email, None)
        return jsonify({"error": "Code expiré (10 min). Veuillez recommencer l'inscription."}), 410

    # Limiter les tentatives (max 5)
    pending["attempts"] += 1
    if pending["attempts"] > 5:
        _pending_signups.pop(email, None)
        return jsonify({"error": "Trop de tentatives. Veuillez recommencer l'inscription."}), 429

    # Vérifier le code
    if code != pending["code"]:
        remaining = 5 - pending["attempts"]
        return jsonify({
            "error":     f"Code incorrect. {remaining} tentative{'s' if remaining > 1 else ''} restante{'s' if remaining > 1 else ''}.",
            "remaining": remaining,
        }), 401

    # Code correct → créer le compte
    conn = None
    try:
        conn = get_db(); cur = conn.cursor()
        # Double-check : l'email n'a pas été créé entre temps
        cur.execute("SELECT id FROM users_profiles WHERE email = %s", (email,))
        if cur.fetchone():
            _pending_signups.pop(email, None)
            return jsonify({"error": "Cet email est déjà utilisé."}), 409

        cur.execute(
            "INSERT INTO users_profiles (email, password_hash, selected_zones, plan) "
            "VALUES (%s, %s, %s, %s) RETURNING id",
            (email, pending["password_hash"], json.dumps([]), 'free')
        )
        user_id = cur.fetchone()[0]
        conn.commit()

        # Nettoyer l'entrée en attente
        _pending_signups.pop(email, None)

        token = create_session(user_id)
        print(f"[Auth] Compte créé et vérifié : {email} (id={user_id})")
        return jsonify({
            "token": token,
            "user":  {"id": user_id, "email": email, "plan": "free", "zones_count": 0},
        }), 201

    except Exception as e:
        if conn: conn.rollback()
        print(f"[Auth] Erreur création compte {email}: {e}")
        return jsonify({"error": "Erreur serveur."}), 500
    finally:
        if conn: conn.close()

@app.route('/api/auth/forgot-password', methods=['POST'])
def forgot_password():
    """
    Demande de réinitialisation de mot de passe.
    Génère un token sécurisé, envoie un email avec le lien.
    Réponse toujours générique pour ne pas révéler si l'email existe.
    """
    data  = request.json or {}
    email = (data.get('email') or '').strip().lower()
    if not email:
        return jsonify({"error": "Email requis."}), 400

    # Nettoyage des tokens expirés
    now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    expired_tokens = [t for t, v in _reset_tokens.items() if v["expires_at"] < now]
    for t in expired_tokens:
        _reset_tokens.pop(t, None)

    # Vérifier si l'email existe en DB (en silence — on répond pareil dans tous les cas)
    conn = None
    user_id = None
    try:
        conn = get_db(); cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT id FROM users_profiles WHERE email = %s", (email,))
        row = cur.fetchone()
        if row:
            user_id = row["id"]
    except Exception:
        pass
    finally:
        if conn: conn.close()

    # Si l'email existe, générer et envoyer le token
    if user_id:
        token = secrets.token_urlsafe(32)   # 256 bits d'entropie
        _reset_tokens[token] = {
            "user_id":    user_id,
            "email":      email,
            "expires_at": now + datetime.timedelta(minutes=30),
        }
        reset_link = f"{APP_BASE_URL}/reset-password?token={token}"
        send_reset_email(email, reset_link)
        print(f"[Auth] Token reset généré pour {email} (user_id={user_id})")
    else:
        # Email inconnu : on ne fait rien mais on répond pareil (anti-énumération)
        print(f"[Auth] Demande reset pour email inconnu : {email}")

    # Réponse générique dans tous les cas
    return jsonify({
        "status":  "ok",
        "message": "Si un compte existe pour cet email, vous recevrez un lien de réinitialisation dans quelques minutes.",
    }), 200


@app.route('/api/auth/check-reset-token', methods=['GET'])
def check_reset_token():
    """
    Vérifie qu'un token de reset est valide et non expiré.
    Appelé par le frontend quand l'utilisateur arrive sur la page de reset.
    """
    token = request.args.get('token', '').strip()
    if not token:
        return jsonify({"valid": False, "error": "Token manquant."}), 400

    entry = _reset_tokens.get(token)
    if not entry:
        return jsonify({"valid": False, "error": "Lien invalide ou déjà utilisé."}), 404

    if datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) > entry["expires_at"]:
        _reset_tokens.pop(token, None)
        return jsonify({"valid": False, "error": "Lien expiré (30 min). Recommencez la demande."}), 410

    return jsonify({"valid": True, "email": entry["email"]}), 200


@app.route('/api/auth/reset-password', methods=['POST'])
def reset_password():
    """
    Réinitialise le mot de passe avec un token valide.
    Le token est supprimé après utilisation (usage unique).
    """
    data     = request.json or {}
    token    = (data.get('token') or '').strip()
    new_pw   = data.get('password', '')

    if not token or not new_pw:
        return jsonify({"error": "Token et nouveau mot de passe requis."}), 400

    # Valider le mot de passe
    ok, msg = validate_password(new_pw)
    if not ok:
        return jsonify({"error": msg}), 400

    # Vérifier le token
    entry = _reset_tokens.get(token)
    if not entry:
        return jsonify({"error": "Lien invalide ou déjà utilisé."}), 404

    if datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) > entry["expires_at"]:
        _reset_tokens.pop(token, None)
        return jsonify({"error": "Lien expiré (30 min). Recommencez la demande."}), 410

    user_id = entry["user_id"]
    email   = entry["email"]

    # Mettre à jour le mot de passe en DB
    conn = None
    try:
        conn = get_db(); cur = conn.cursor()
        new_hash = generate_password_hash(new_pw)
        cur.execute(
            "UPDATE users_profiles SET password_hash = %s WHERE id = %s",
            (new_hash, user_id)
        )
        conn.commit()

        # Invalider le token (usage unique)
        _reset_tokens.pop(token, None)

        # Invalider toutes les sessions actives de cet utilisateur (sécurité)
        stale = [t for t, uid in _sessions.items() if uid == user_id]
        for t in stale:
            _sessions.pop(t, None)

        print(f"[Auth] Mot de passe réinitialisé pour {email} (user_id={user_id})")
        return jsonify({"status": "ok", "message": "Mot de passe mis à jour. Vous pouvez maintenant vous connecter."}), 200

    except Exception as e:
        if conn: conn.rollback()
        print(f"[Auth] Erreur reset password {email}: {e}")
        return jsonify({"error": "Erreur serveur."}), 500
    finally:
        if conn: conn.close()


@app.route('/reset-password')
def reset_password_page():
    """Sert index.html pour que le frontend gère le token via ?token=XXX."""
    return send_from_directory('.', 'index.html')


@app.route('/api/auth/login', methods=['POST'])
def login():
    data=request.json or {}; email=(data.get('email')or'').strip().lower(); password=data.get('password','')
    if not email or not password: return jsonify({"error":"Email et mot de passe requis."}),400
    conn=None
    try:
        conn=get_db(); cur=conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT id,email,password_hash,selected_zones FROM users_profiles WHERE email = %s",(email,))
        user=cur.fetchone()
        if not user or not check_password_hash(user['password_hash'],password):
            return jsonify({"error":"Email ou mot de passe incorrect."}),401
        plan=get_plan(user['id']); token=create_session(user['id'])
        zones=user.get('selected_zones') or []
        if isinstance(zones,str): zones=json.loads(zones)
        return jsonify({"token":token,"user":{"id":user['id'],"email":user['email'],"plan":plan,"zones_count":len(zones)}})
    except Exception as e:
        return jsonify({"error":"Erreur serveur."}),500
    finally:
        if conn: conn.close()

@app.route('/api/auth/logout', methods=['POST'])
def logout():
    auth=request.headers.get('Authorization',''); token=auth.replace('Bearer ','').strip()
    _sessions.pop(token,None); return jsonify({"status":"ok"})

@app.route('/api/auth/change-email', methods=['PATCH'])
@require_auth
def change_email():
    """Change l'email de l'utilisateur après vérification du mot de passe."""
    user     = request.current_user
    data     = request.json or {}
    new_email = (data.get('email') or '').strip().lower()
    password  = data.get('password', '')
    if not new_email or not password:
        return jsonify({"error": "Nouvel email et mot de passe requis."}), 400
    if not re.match(r'^[^@]+@[^@]+\.[^@]+$', new_email):
        return jsonify({"error": "Format d'email invalide."}), 400
    conn = None
    try:
        conn = get_db(); cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT password_hash FROM users_profiles WHERE id = %s", (user['id'],))
        row = cur.fetchone()
        if not row or not check_password_hash(row['password_hash'], password):
            return jsonify({"error": "Mot de passe incorrect."}), 401
        cur.execute("SELECT id FROM users_profiles WHERE email = %s AND id != %s",
                    (new_email, user['id']))
        if cur.fetchone():
            return jsonify({"error": "Cet email est déjà utilisé."}), 409
        cur.execute("UPDATE users_profiles SET email = %s WHERE id = %s",
                    (new_email, user['id']))
        conn.commit()
        print(f"[Auth] Email modifié pour user {user['id']} → {new_email}")
        return jsonify({"status": "ok", "email": new_email}), 200
    except Exception as e:
        if conn: conn.rollback()
        return jsonify({"error": "Erreur serveur."}), 500
    finally:
        if conn: conn.close()

@app.route('/api/auth/change-password', methods=['PATCH'])
@require_auth
def change_password():
    """Change le mot de passe après vérification de l'ancien."""
    user    = request.current_user
    data    = request.json or {}
    old_pw  = data.get('old_password', '')
    new_pw  = data.get('new_password', '')
    if not old_pw or not new_pw:
        return jsonify({"error": "Ancien et nouveau mot de passe requis."}), 400
    ok, msg = validate_password(new_pw)
    if not ok: return jsonify({"error": msg}), 400
    conn = None
    try:
        conn = get_db(); cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT password_hash FROM users_profiles WHERE id = %s", (user['id'],))
        row = cur.fetchone()
        if not row or not check_password_hash(row['password_hash'], old_pw):
            return jsonify({"error": "Mot de passe actuel incorrect."}), 401
        cur.execute("UPDATE users_profiles SET password_hash = %s WHERE id = %s",
                    (generate_password_hash(new_pw), user['id']))
        conn.commit()
        print(f"[Auth] Mot de passe modifié pour user {user['id']}")
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        if conn: conn.rollback()
        return jsonify({"error": "Erreur serveur."}), 500
    finally:
        if conn: conn.close()

@app.route('/api/auth/delete-account', methods=['DELETE'])
@require_auth
def delete_account():
    """
    Supprime définitivement le compte et toutes les données de l'utilisateur.
    - Invalide toutes ses sessions actives
    - Supprime la ligne dans users_profiles (cascade sur les données)
    Requiert confirmation du mot de passe dans le body JSON.
    """
    user     = request.current_user
    data     = request.json or {}
    password = data.get('password', '')

    if not password:
        return jsonify({"error": "Mot de passe requis pour confirmer la suppression."}), 400

    conn = None
    try:
        conn = get_db(); cur = conn.cursor(cursor_factory=RealDictCursor)
        # Vérifier le mot de passe avant suppression
        cur.execute("SELECT password_hash FROM users_profiles WHERE id = %s", (user['id'],))
        row = cur.fetchone()
        if not row or not check_password_hash(row['password_hash'], password):
            return jsonify({"error": "Mot de passe incorrect."}), 401

        # Invalider toutes les sessions de l'utilisateur
        stale_tokens = [t for t, uid in _sessions.items() if uid == user['id']]
        for t in stale_tokens:
            _sessions.pop(t, None)

        # Supprimer le compte (toutes les données dans la colonne selected_zones incluses)
        cur.execute("DELETE FROM users_profiles WHERE id = %s", (user['id'],))
        conn.commit()

        print(f"[Auth] Compte supprimé : {user['email']} (id={user['id']})")
        return jsonify({"status": "ok", "message": "Compte supprimé définitivement."}), 200

    except Exception as e:
        if conn: conn.rollback()
        print(f"[Auth] Erreur suppression compte {user.get('email', '?')}: {e}")
        return jsonify({"error": "Erreur serveur."}), 500
    finally:
        if conn: conn.close()

@app.route('/api/auth/me', methods=['GET'])
@require_auth
def me():
    user=request.current_user; conn=None
    try:
        conn=get_db(); cur=conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT selected_zones, preferences FROM users_profiles WHERE id = %s",(user['id'],))
        row=cur.fetchone()
        zones=row['selected_zones'] if row else []
        if isinstance(zones,str): zones=json.loads(zones)
        prefs=row['preferences'] if row else {}
        if isinstance(prefs,str): prefs=json.loads(prefs)
        if not prefs: prefs={"lang":"fr","unit":"metric"}
        plan=get_plan(user['id'])
        return jsonify({"user":{**dict(user),"plan":plan,"selected_zones":zones,"zones_count":len(zones),"preferences":prefs}})
    except Exception as e: return jsonify({"error":str(e)}),500
    finally:
        if conn: conn.close()

@app.route('/api/user/preferences', methods=['PATCH'])
@require_auth
def update_preferences():
    """Met à jour les préférences utilisateur (langue, unité de mesure)."""
    user = request.current_user
    data = request.json or {}
    lang = data.get('lang')
    unit = data.get('unit')
    # Valeurs autorisées
    if lang and lang not in ('fr', 'en', 'es', 'de', 'pt', 'ar'):
        return jsonify({"error": "Langue non supportée."}), 400
    if unit and unit not in ('metric', 'imperial'):
        return jsonify({"error": "Unité non supportée."}), 400
    conn = None
    try:
        conn = get_db(); cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT preferences FROM users_profiles WHERE id = %s", (user['id'],))
        row = cur.fetchone()
        prefs = row['preferences'] if row else {}
        if isinstance(prefs, str): prefs = json.loads(prefs)
        if not prefs: prefs = {"lang": "fr", "unit": "metric"}
        if lang: prefs['lang'] = lang
        if unit: prefs['unit'] = unit
        cur.execute("UPDATE users_profiles SET preferences = %s WHERE id = %s",
                    (json.dumps(prefs), user['id']))
        conn.commit()
        return jsonify({"status": "ok", "preferences": prefs})
    except Exception as e:
        if conn: conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

@app.route('/api/user/export', methods=['GET'])
@require_auth
def export_user_data():
    """
    Exporte toutes les données de l'utilisateur en JSON (RGPD).
    Contient : email, plan, préférences, parcelles enregistrées.
    """
    user = request.current_user
    conn = None
    try:
        conn = get_db(); cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            "SELECT email, plan, preferences, selected_zones, created_at "
            "FROM users_profiles WHERE id = %s",
            (user['id'],)
        )
        row = cur.fetchone()
        if not row:
            return jsonify({"error": "Utilisateur introuvable."}), 404

        zones = row['selected_zones'] or []
        if isinstance(zones, str): zones = json.loads(zones)
        prefs = row['preferences'] or {}
        if isinstance(prefs, str): prefs = json.loads(prefs)

        export = {
            "parisk_export": {
                "version": "1.0",
                "exported_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "user": {
                    "id":         user['id'],
                    "email":      row['email'],
                    "plan":       row['plan'],
                    "created_at": row['created_at'].isoformat() if row['created_at'] else None,
                    "preferences": prefs,
                },
                "parcelles": [
                    {
                        "id":       p.get('id'),
                        "label":    p.get('label'),
                        "surface":  p.get('surface'),
                        "cultures": p.get('cultures', []),
                        "geometry": p.get('geometry'),
                        "savedAt":  p.get('savedAt'),
                    }
                    for p in zones if isinstance(p, dict)
                ],
                "stats": {
                    "total_parcelles": len(zones),
                    "surface_totale_ha": round(
                        sum(float(p.get('surface') or 0) for p in zones if isinstance(p, dict)), 2
                    ),
                }
            }
        }

        from flask import Response
        resp = Response(
            json.dumps(export, ensure_ascii=False, indent=2),
            mimetype='application/json',
            headers={
                'Content-Disposition': f'attachment; filename="parisk_export_{user["id"]}.json"'
            }
        )
        return resp
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

# ============================================================
# PARCELLES ROUTES
# ============================================================
@app.route('/api/parcelles/saved', methods=['GET'])
@require_auth
def get_saved_parcels():
    user=request.current_user; conn=None
    try:
        conn=get_db(); cur=conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT selected_zones FROM users_profiles WHERE id = %s",(user['id'],))
        row=cur.fetchone(); zones=row['selected_zones'] if row else []
        if isinstance(zones,str): zones=json.loads(zones)
        plan=get_plan(user['id'])
        return jsonify({"parcels":zones or[],"plan":plan,"count":len(zones or[])})
    except Exception as e: return jsonify({"error":str(e)}),500
    finally:
        if conn: conn.close()

@app.route('/api/parcelles/saved', methods=['POST'])
@require_auth
def save_parcels():
    user=request.current_user; payload=request.json or {}; new_parcels=payload.get('parcels',[])
    if not isinstance(new_parcels,list): return jsonify({"error":"Format invalide."}),400
    conn=None
    try:
        conn=get_db(); cur=conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT selected_zones FROM users_profiles WHERE id = %s",(user['id'],))
        row=cur.fetchone(); plan=get_plan(user['id'])
        existing=row['selected_zones'] if row else []
        if isinstance(existing,str): existing=json.loads(existing)
        existing=existing or []; limit=FREE_PLAN_LIMIT if plan=='free' else 999999
        existing_ids={p['id'] for p in existing if isinstance(p,dict) and 'id' in p}
        def geom_hash(p):
            g = p.get('geometry')
            if not g: return None
            return hash(json.dumps(g, sort_keys=True))
        existing_geom_hashes = {geom_hash(p) for p in existing if geom_hash(p) is not None}
        added=0; skipped_limit=0; skipped_dup=0
        for parcel in new_parcels:
            if not isinstance(parcel,dict) or 'id' not in parcel: continue
            if parcel['id'] in existing_ids: skipped_dup+=1; continue
            gh = geom_hash(parcel)
            if gh and gh in existing_geom_hashes: skipped_dup+=1; continue
            if len(existing)>=limit: skipped_limit+=1; continue
            existing.append(parcel); existing_ids.add(parcel['id'])
            if gh: existing_geom_hashes.add(gh)
            added+=1
        cur2=conn.cursor()
        cur2.execute("UPDATE users_profiles SET selected_zones = %s WHERE id = %s",(json.dumps(existing),user['id']))
        conn.commit()
        return jsonify({"status":"ok","count":len(existing),"added":added,"skipped_limit":skipped_limit,"skipped_duplicate":skipped_dup,"limit":limit})
    except Exception as e:
        if conn: conn.rollback()
        return jsonify({"error":str(e)}),500
    finally:
        if conn: conn.close()

@app.route('/api/parcelles/saved/<parcel_id>', methods=['DELETE'])
@require_auth
def delete_parcel(parcel_id):
    user=request.current_user; conn=None
    try:
        conn=get_db(); cur=conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT selected_zones FROM users_profiles WHERE id = %s",(user['id'],))
        row=cur.fetchone(); zones=row['selected_zones'] if row else []
        if isinstance(zones,str): zones=json.loads(zones)
        zones=[z for z in (zones or[]) if z.get('id')!=parcel_id]
        cur2=conn.cursor()
        cur2.execute("UPDATE users_profiles SET selected_zones = %s WHERE id = %s",(json.dumps(zones),user['id']))
        conn.commit(); return jsonify({"status":"ok","count":len(zones)})
    except Exception as e:
        if conn: conn.rollback()
        return jsonify({"error":str(e)}),500
    finally:
        if conn: conn.close()

@app.route('/api/parcelles/saved/<parcel_id>', methods=['PATCH'])
@require_auth
def update_parcel(parcel_id):
    user=request.current_user; data=request.json or {}
    new_label = data.get('label', '').strip()
    new_cultures = data.get('cultures')
    conn=None
    try:
        conn=get_db(); cur=conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT selected_zones FROM users_profiles WHERE id = %s",(user['id'],))
        row=cur.fetchone(); zones=row['selected_zones'] if row else []
        if isinstance(zones,str): zones=json.loads(zones)
        found=False
        for z in (zones or[]):
            if z.get('id')==parcel_id:
                if new_label: z['label'] = new_label
                if new_cultures is not None: z['cultures'] = new_cultures
                found=True; break
        if not found: return jsonify({"error":"Parcelle non trouvée."}),404
        cur2=conn.cursor()
        cur2.execute("UPDATE users_profiles SET selected_zones = %s WHERE id = %s",(json.dumps(zones),user['id']))
        conn.commit(); return jsonify({"status":"ok"})
    except Exception as e:
        if conn: conn.rollback()
        return jsonify({"error":str(e)}),500
    finally:
        if conn: conn.close()

# ============================================================
# STARTUP — fonctionne avec python app.py ET gunicorn app:app
# ============================================================
def _startup():
    """
    Initialise l'application au démarrage.
    - Vérifie/crée les tables DB
    - Charge TRI et MVT en mémoire
    - Charge vigilance_active.geojson en cache mémoire
      (le fichier est mis à jour toutes les 5h par GitHub Actions)

    Guard WERKZEUG_RUN_MAIN : évite la double exécution avec le reloader Werkzeug.
    """
    # En mode debug Werkzeug, ne s'exécuter que dans le processus worker
    run_main = os.environ.get('WERKZEUG_RUN_MAIN')
    if run_main is not None and run_main != 'true':
        return  # processus reloader → skip

    ensure_tables()
    load_tri()
    load_mvt()
    update_vigilance()  # charge le fichier GH Actions en cache mémoire


# Exécution à l'import du module → compatible Gunicorn ET python app.py
_startup()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=True)
