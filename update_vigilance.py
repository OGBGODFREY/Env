"""
update_vigilance.py — Script autonome pour GitHub Actions
=========================================================
Appelé toutes les 5h par le workflow .github/workflows/main.yml.
1. Appelle l'API Météo-France DPVigilance
2. Parse les alertes par département
3. Enrichit vigilance_active.geojson (gabarit de base dans le repo)
4. Écrit le fichier → GitHub Actions commit + push automatiquement

Variables d'environnement requises (GitHub Secrets) :
  MF_API_KEY  → clé JWT Météo-France
"""

import json
import os
import sys
import datetime
import requests

# ── Config ────────────────────────────────────────────────────
MF_API_KEY  = os.environ.get("MF_API_KEY", "")
MF_VIGI_URL = "https://public-api.meteofrance.fr/public/DPVigilance/v1/cartevigilance/encours"

# Le fichier est à la racine du repo (même dossier que ce script)
SCRIPT_DIR       = os.path.dirname(os.path.abspath(__file__))
VIGILANCE_OUTPUT = os.path.join(SCRIPT_DIR, "vigilance_active.geojson")

# ── Référentiels Météo-France ─────────────────────────────────
PHENOM_NAMES = {
    1: "Vent violent", 2: "Pluie-inondation", 3: "Orages",
    4: "Crues",        5: "Neige-verglas",    6: "Canicule",
    7: "Grand froid",  8: "Avalanches",       9: "Vagues-submersion",
}
PHENOM_ICONS = {
    1: "💨", 2: "🌧️", 3: "⛈️", 4: "🌊", 5: "❄️",
    6: "🌡️", 7: "🥶", 8: "🏔️", 9: "🌊",
}
VIGI_COLORS = {1: "vert", 2: "jaune", 3: "orange", 4: "rouge"}
VIGI_HEX    = {1: "#1e8449", 2: "#d97706", 3: "#c2410c", 4: "#b91c1c"}


def _dept_from_domain_id(domain_id):
    """Valide et normalise un domain_id en code département (ex: "02", "2A")."""
    if domain_id is None:
        return None
    s = str(domain_id).strip()
    if s in ("FRA", "99", ""):
        return None
    if len(s) >= 4 and s.isdigit():   # zones marines ex: "3010"
        return None
    if s in ("2A", "2B"):
        return s
    if s.isdigit():
        n = int(s)
        return s.zfill(2) if 1 <= n <= 95 else None
    return None


def parse_mf_alerts(raw):
    """Parse la réponse DPVigilance v1 → liste d'alertes par (dept, phénomène)."""
    alerts_map = {}
    if not raw:
        return []

    for period in raw.get("product", {}).get("periods", []):
        echeance   = period.get("echeance", "")
        begin_time = period.get("begin_validity_time", "")
        end_time   = period.get("end_validity_time", "")
        for d in period.get("timelaps", {}).get("domain_ids", []):
            dept = _dept_from_domain_id(d.get("domain_id"))
            if not dept:
                continue
            max_color = int(d.get("max_color_id", 1) or 1)
            if max_color < 2:
                continue
            for ph in d.get("phenomenon_items", []):
                ph_color = int(ph.get("phenomenon_max_color_id", 1) or 1)
                if ph_color < 2:
                    continue
                try:
                    ph_id = int(ph.get("phenomenon_id"))
                except (ValueError, TypeError):
                    continue
                key = (dept, ph_id)
                if key not in alerts_map or ph_color > alerts_map[key]["level"]:
                    alerts_map[key] = {
                        "dept":       dept,
                        "domain_id":  str(d.get("domain_id")),
                        "level":      ph_color,
                        "phenomenon": ph_id,
                        "colorName":  VIGI_COLORS.get(ph_color, "jaune"),
                        "colorHex":   VIGI_HEX.get(ph_color, "#d97706"),
                        "phenomName": PHENOM_NAMES.get(ph_id, "Vigilance"),
                        "phenomIcon": PHENOM_ICONS.get(ph_id, "⚠️"),
                        "dateDebut":  begin_time,
                        "dateFin":    end_time,
                        "echeance":   echeance,
                    }

    result = sorted(alerts_map.values(), key=lambda x: (x["dept"], -x["level"]))
    print(
        f"[MF] {len(result)} alertes — "
        f"{sum(1 for a in result if a['level']==4)} rouge  "
        f"{sum(1 for a in result if a['level']==3)} orange  "
        f"{sum(1 for a in result if a['level']==2)} jaune"
    )
    print(f"[MF] Depts en alerte : {sorted(set(a['dept'] for a in result))}")
    return result


def build_enriched_geojson(base_features, alerts, updated_at):
    """Enrichit chaque feature département avec les alertes correspondantes."""
    alerts_by_dept = {}
    for a in alerts:
        alerts_by_dept.setdefault(a["dept"], []).append(a)
    max_level_by_dept = {
        dept: max(a["level"] for a in lst)
        for dept, lst in alerts_by_dept.items()
    }

    enriched = []
    for feat in base_features:
        props = feat["properties"].copy()
        code  = props["code"].strip()
        if code.isdigit():
            code = code.zfill(2)
        dept_alerts = alerts_by_dept.get(code, [])
        max_lvl     = max_level_by_dept.get(code, 1)
        props.update({
            "dept_num":       code,
            "vigi_level":     max_lvl,
            "vigi_colorName": VIGI_COLORS.get(max_lvl, "vert"),
            "vigi_colorHex":  VIGI_HEX.get(max_lvl, "#1e8449"),
            "vigi_alerts":    dept_alerts,
            "updated_at":     updated_at,
        })
        enriched.append({
            "type":       "Feature",
            "properties": props,
            "geometry":   feat["geometry"],
        })
    return {
        "type":       "FeatureCollection",
        "features":   enriched,
        "updated_at": updated_at,
    }


def main():
    print("=" * 60)
    print("Parisk — Mise à jour vigilance Météo-France")
    print("=" * 60)

    # 1. Vérifier la clé API
    if not MF_API_KEY:
        print("ERREUR : variable d'environnement MF_API_KEY manquante.")
        sys.exit(1)

    # 2. Charger le gabarit de base (points GPS des 95 départements)
    if not os.path.exists(VIGILANCE_OUTPUT):
        print(f"ERREUR : fichier de base introuvable : {VIGILANCE_OUTPUT}")
        sys.exit(1)

    with open(VIGILANCE_OUTPUT, "r", encoding="utf-8") as f:
        base_gj = json.load(f)

    base_features = []
    for feat in base_gj.get("features", []):
        props = feat.get("properties", {})
        code  = props.get("code", "")
        dept_name = props.get("departement", "")
        if not code and not dept_name:
            continue
        base_features.append({
            "type":       "Feature",
            "properties": {"code": str(code).strip(), "departement": dept_name},
            "geometry":   feat.get("geometry"),
        })

    print(f"[Base] {len(base_features)} départements chargés")

    if not base_features:
        print("ERREUR : aucun département dans le fichier de base.")
        sys.exit(1)

    # 3. Appel API Météo-France
    print(f"[MF] Appel API : {MF_VIGI_URL}")
    try:
        resp = requests.get(
            MF_VIGI_URL,
            headers={"apikey": MF_API_KEY, "Accept": "application/json"},
            timeout=30,
        )
        print(f"[MF] HTTP {resp.status_code}")
        if resp.status_code != 200:
            print(f"ERREUR API : HTTP {resp.status_code}\n{resp.text[:300]}")
            sys.exit(1)
        raw = resp.json()
    except requests.RequestException as e:
        print(f"ERREUR réseau : {e}")
        sys.exit(1)

    # 4. Parser les alertes
    alerts = parse_mf_alerts(raw)

    # 5. Construire le GeoJSON enrichi
    updated_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    enriched_gj = build_enriched_geojson(base_features, alerts, updated_at)

    # 6. Écrire le fichier
    with open(VIGILANCE_OUTPUT, "w", encoding="utf-8") as f:
        json.dump(enriched_gj, f, ensure_ascii=False, indent=None)

    n_alert = sum(
        1 for feat in enriched_gj["features"]
        if feat["properties"].get("vigi_level", 1) >= 2
    )
    print(f"[OK] {len(enriched_gj['features'])} départements écrits "
          f"dont {n_alert} en vigilance")
    print(f"[OK] updated_at = {updated_at}")
    print(f"[OK] Fichier : {VIGILANCE_OUTPUT}")
    print("=" * 60)


if __name__ == "__main__":
    main()
