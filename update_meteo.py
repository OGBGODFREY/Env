"""
update_meteo.py — Script autonome pour GitHub Actions
======================================================
Appelé par le workflow .github/workflows/update_meteo.yml.
Stratégie 5 batches (1/5 des EPCI par run) pour respecter
les limites de l'API Open-Meteo (gratuite, ~10 000 req/min).

Variables d'environnement (GitHub Secrets / vars) :
  BATCH_INDEX  → 0 | 1 | 2 | 3 | 4  (injecté par la matrice du workflow)

Fichier mis à jour en place : points_epci_meteo.geojson
"""

import json
import math
import os
import sys
import time
import datetime

import numpy as np
import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry

# ── Config ────────────────────────────────────────────────────
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
SCRIPT_DIR     = os.path.dirname(os.path.abspath(__file__))
EPCI_PATH      = os.path.join(SCRIPT_DIR, "points_epci_meteo.geojson")
NB_BATCHES     = 5          # 5 runs de 5 min chacun = mise à jour complète en 20 min
SLEEP_BETWEEN  = 0.45       # secondes entre chaque appel API (≈ 2,2 req/s)

# ── Client Open-Meteo avec cache + retry ─────────────────────
_cache_session = requests_cache.CachedSession(".cache_gh", expire_after=3600)
_retry_session = retry(_cache_session, retries=5, backoff_factor=0.3)
openmeteo_client = openmeteo_requests.Client(session=_retry_session)


# ── Référentiels WMO ─────────────────────────────────────────
WMO_CODES = {
    0:  ("Ciel dégagé",        "☀️"),
    1:  ("Peu nuageux",        "🌤️"),
    2:  ("Partiellement nuageux","⛅"),
    3:  ("Couvert",            "☁️"),
    45: ("Brouillard",         "🌫️"),
    48: ("Brouillard givrant",  "🌫️"),
    51: ("Bruine légère",      "🌦️"),
    53: ("Bruine modérée",     "🌦️"),
    55: ("Bruine forte",       "🌧️"),
    61: ("Pluie faible",       "🌧️"),
    63: ("Pluie modérée",      "🌧️"),
    65: ("Pluie forte",        "🌧️"),
    71: ("Neige faible",       "❄️"),
    73: ("Neige modérée",      "🌨️"),
    75: ("Neige forte",        "🌨️"),
    77: ("Grésil",             "🌨️"),
    80: ("Averses faibles",    "🌦️"),
    81: ("Averses modérées",   "🌧️"),
    82: ("Averses violentes",  "⛈️"),
    85: ("Averses de neige",   "🌨️"),
    86: ("Averses de neige forte","🌨️"),
    95: ("Orage",              "⛈️"),
    96: ("Orage avec grêle",   "⛈️"),
    99: ("Orage violent grêle","⛈️"),
}

def get_wmo_label(code: int):
    return WMO_CODES.get(int(code), ("Inconnu", "🌡️"))

DIRECTIONS = ["N","NNE","NE","ENE","E","ESE","SE","SSE",
              "S","SSO","SO","OSO","O","ONO","NO","NNO"]

def deg_to_arrow(deg: int) -> str:
    idx = round((deg % 360) / 22.5) % 16
    arrows = ["↓","↙","↙","↙","←","↖","↖","↖","↑","↗","↗","↗","→","↘","↘","↘"]
    return arrows[idx]


# ── Fetch météo pour un point EPCI ───────────────────────────
def _fetch_meteo_for_point(feat):
    """Appelle Open-Meteo pour un point EPCI → (uid, dict | None)."""
    props  = feat.get("properties", {})
    uid    = props.get("uid", "?")
    coords = feat.get("geometry", {}).get("coordinates", [])
    if len(coords) < 2:
        return uid, None
    lon, lat = coords[0], coords[1]

    params = {
        "latitude":      lat,
        "longitude":     lon,
        "timezone":      "Europe/Paris",
        "forecast_days": 7,
        "hourly": [
            "temperature_2m", "precipitation", "relative_humidity_2m",
            "wind_speed_10m", "wind_direction_10m", "cloud_cover",
            "dew_point_2m", "weather_code", "soil_moisture_0_to_1cm",
            "et0_fao_evapotranspiration",
        ],
        "daily": [
            "weather_code", "temperature_2m_max", "temperature_2m_min",
            "precipitation_sum", "wind_speed_10m_max",
            "wind_direction_10m_dominant", "relative_humidity_2m_max",
            "relative_humidity_2m_min", "dew_point_2m_mean",
            "cloud_cover_mean", "et0_fao_evapotranspiration_sum",
            "precipitation_probability_max",
        ],
        "current": [
            "temperature_2m", "relative_humidity_2m", "precipitation",
            "wind_speed_10m", "wind_direction_10m", "weather_code",
            "cloud_cover", "dew_point_2m",
        ],
    }

    def _process(responses):
        response = responses[0]

        # ── Current ──────────────────────────────────────────
        cur_r = response.Current()
        def cv(i):
            try:   return round(float(cur_r.Variables(i).Value()), 2)
            except: return None
        cur_wcode = int(cv(5) or 0)
        wl, wi = get_wmo_label(cur_wcode)
        current = {
            "temperature":   cv(0),
            "humidity":      cv(1),
            "precipitation": cv(2),
            "wind_speed":    cv(3),
            "wind_dir":      cv(4),
            "wind_arrow":    deg_to_arrow(int(cv(4) or 0)),
            "cloud_cover":   cv(6),
            "dew_point":     cv(7),
            "weather_code":  cur_wcode,
            "weather_label": wl,
            "weather_icon":  wi,
        }

        # ── Hourly (jour J uniquement pour alléger) ───────────
        hourly_r = response.Hourly()
        HOURLY_KEYS = ["temperature", "precipitation", "humidity", "wind_speed",
                       "wind_dir", "cloud_cover", "dew_point", "weather_code",
                       "soil_moisture", "et0"]
        hourly_arrays = {key: hourly_r.Variables(i).ValuesAsNumpy()
                         for i, key in enumerate(HOURLY_KEYS)}
        utc_offset_s = response.UtcOffsetSeconds()
        times_ts = pd.date_range(
            start=pd.to_datetime(hourly_r.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly_r.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly_r.Interval()), inclusive="left",
        )
        times_local = times_ts + pd.Timedelta(seconds=utc_offset_s)
        today_str   = datetime.date.today().isoformat()
        hourly_day  = {key: [] for key in HOURLY_KEYS}
        hourly_day["times"] = []
        for i, ts in enumerate(times_local):
            if ts.strftime("%Y-%m-%d") == today_str:
                hourly_day["times"].append(ts.strftime("%H:%M"))
                for key in HOURLY_KEYS:
                    v = hourly_arrays[key][i]
                    hourly_day[key].append(None if np.isnan(v) else round(float(v), 2))

        # ── Daily (7 jours) ───────────────────────────────────
        daily_r = response.Daily()
        DAILY_KEYS = ["weather_code", "temp_max", "temp_min", "precipitation",
                      "wind_speed_max", "wind_dir_dominant", "humidity_max",
                      "humidity_min", "dew_point", "cloud_cover", "et0",
                      "precip_prob_max"]
        daily_arrays = {key: daily_r.Variables(i).ValuesAsNumpy()
                        for i, key in enumerate(DAILY_KEYS)}
        daily_dates = pd.date_range(
            start=pd.to_datetime(daily_r.Time(), unit="s", utc=True),
            end=pd.to_datetime(daily_r.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=daily_r.Interval()), inclusive="left",
        )
        day_names_fr = ["Lun.", "Mar.", "Mer.", "Jeu.", "Ven.", "Sam.", "Dim."]
        daily_list   = []
        for i, ts in enumerate(daily_dates):
            code       = int(daily_arrays["weather_code"][i] or 0)
            wl2, wi2   = get_wmo_label(code)
            wind_deg   = daily_arrays["wind_dir_dominant"][i]
            def dv(key, _i=i):
                v = daily_arrays[key][_i]
                return None if np.isnan(v) else round(float(v), 2)
            daily_list.append({
                "date":          ts.strftime("%Y-%m-%d"),
                "day_name":      "Aujourd'hui" if i == 0 else day_names_fr[ts.weekday()],
                "day_fmt":       ts.strftime("%d/%m"),
                "weather_code":  code,
                "weather_label": wl2,
                "weather_icon":  wi2,
                "temp_max":      dv("temp_max"),
                "temp_min":      dv("temp_min"),
                "precipitation": dv("precipitation"),
                "wind_speed":    dv("wind_speed_max"),
                "wind_dir":      dv("wind_dir_dominant"),
                "wind_arrow":    deg_to_arrow(int(wind_deg or 0) if not np.isnan(wind_deg) else 0),
                "humidity_max":  dv("humidity_max"),
                "humidity_min":  dv("humidity_min"),
                "cloud_cover":   dv("cloud_cover"),
                "dew_point":     dv("dew_point"),
                "et0":           dv("et0"),
                "precip_prob_max": dv("precip_prob_max"),
            })

        return {
            "current":      current,
            "hourly":       hourly_day,
            "daily":        daily_list,
            "collected_at": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

    try:
        responses = openmeteo_client.weather_api(OPEN_METEO_URL, params=params)
        return uid, _process(responses)

    except Exception as e:
        err_str = str(e)
        if "Minutely API request limit exceeded" in err_str:
            print(f"[Météo] UID {uid} — rate-limit, attente 65s puis retry…")
            time.sleep(65)
            try:
                responses = openmeteo_client.weather_api(OPEN_METEO_URL, params=params)
                return uid, _process(responses)
            except Exception as e2:
                print(f"[Météo] UID {uid} — erreur après retry : {e2}")
                return uid, None
        print(f"[Météo] UID {uid} — erreur : {e}")
        return uid, None


# ── Batch principal ───────────────────────────────────────────
def run_batch(batch_index: int):
    print("=" * 60)
    print(f"update_meteo.py — batch {batch_index + 1}/{NB_BATCHES}")
    print("=" * 60)

    if not os.path.exists(EPCI_PATH):
        print(f"ERREUR : fichier introuvable → {EPCI_PATH}")
        sys.exit(1)

    with open(EPCI_PATH, "r", encoding="utf-8") as f:
        gj = json.load(f)

    features = gj.get("features", [])
    total    = len(features)
    size     = math.ceil(total / NB_BATCHES)
    start    = batch_index * size
    end      = min(start + size, total)
    batch    = features[start:end]

    print(f"[Info] {total} points EPCI au total — "
          f"ce batch : indices {start}–{end - 1} ({len(batch)} points)")

    ok_count  = 0
    err_count = 0

    for feat in batch:
        uid, meteo = _fetch_meteo_for_point(feat)
        if meteo:
            feat["properties"]["meteo"] = meteo
            ok_count += 1
        else:
            err_count += 1
        time.sleep(SLEEP_BETWEEN)

    # Écriture atomique
    gj["updated_at"]    = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    gj["batch_updated"] = batch_index

    tmp_path = EPCI_PATH + ".tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(gj, f, ensure_ascii=False, indent=None)
        os.replace(tmp_path, EPCI_PATH)
    except Exception as e:
        print(f"ERREUR écriture : {e}")
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        sys.exit(1)

    print(f"[OK] batch {batch_index + 1}/{NB_BATCHES} — "
          f"{ok_count} OK / {err_count} erreurs")
    print(f"[OK] updated_at = {gj['updated_at']}")
    print(f"[OK] Fichier : {EPCI_PATH}")
    print("=" * 60)


# ── Entrypoint ────────────────────────────────────────────────
if __name__ == "__main__":
    raw = os.environ.get("BATCH_INDEX", "").strip()
    if not raw.isdigit() or int(raw) not in range(NB_BATCHES):
        print(f"ERREUR : BATCH_INDEX doit être un entier entre 0 et {NB_BATCHES - 1}.")
        print(f"         Valeur reçue : '{raw}'")
        sys.exit(1)
    run_batch(int(raw))
