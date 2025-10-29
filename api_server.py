from flask import Flask, jsonify, request
import psycopg2
from flask_cors import CORS
import json
import traceback


app = Flask(__name__)
# Active le support CORS pour permettre les requêtes depuis le front-end
CORS(app)


# =======================================================
# Configuration de la Base de Données (Pooler 6543)
# =======================================================
# REMPLACER CES VALEURS PAR VOS PROPRES CREDENTIALS SUPABASE
DB_HOST = "aws-1-eu-west-3.pooler.supabase.com"
DB_NAME = "postgres"
DB_USER = "postgres.czyoeuufsrsmzixxffkg"
DB_PASSWORD = "Godfreyogb02@1"
DB_PORT = "6543"


# Constantes pour les noms de tables de risque
RISK_TABLE_NAME_FLOOD = '"n_carte_inond_s"'
RISK_TABLE_NAME_SEISME = '"Seismes_FR_1950-2025_20251015"'
RISK_TABLE_NAME_BEE = '"bee"'
RISK_TABLE_NAME_SEISME_TA = '"Seisme_Ta_1973_2025"'


# Constante générique pour le MVT de terrain (si vous utilisez une seule table)
RISK_TABLE_NAME_MVT_TERRAIN = '"mvt_georisques_s"' 

# Constantes pour le risque INCENDIE (5 dernières années)
RISK_TABLE_NAME_FIRE_BASE = 'Incendie_FR_Année_'
FIRE_YEARS = [2020, 2021, 2022, 2023, 2024]
FIRE_TABLE_NAMES = [f'"{RISK_TABLE_NAME_FIRE_BASE}{year}"' for year in FIRE_YEARS]


# =======================================================
# Fonctions utilitaires
# =======================================================


def get_db_connection():
    """Crée et retourne une connexion sécurisée (sslmode="require") à la base de données Supabase."""
    conn = psycopg2.connect(
        host=DB_HOST, database=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT, sslmode="require"
    )
    return conn


# =======================================================
# ROUTE D'INTERSECTION (MISE À JOUR CRITIQUE)
# =======================================================
@app.route('/api/intersect', methods=['POST'])
def intersect_risk_data():
    """
    Reçoit une FeatureCollection complète de géométries utilisateur et la couche de risque choisie.
    Effectue une analyse PostGIS pour retourner les résultats (toutes les zones de risque + toutes les zones utilisateur).
    """
    data = request.json
    user_geojson_collection = data.get('user_geometry_collection')
    risk_layer = data.get('risk_layer')
    
    # 1. Détermination du nom de la table ou de la liste de tables
    table_names = None
    is_fire_risk = False
    
    if risk_layer == 'inondation':
        table_names = [RISK_TABLE_NAME_FLOOD]
    elif risk_layer == 'seismes':
        table_names = [RISK_TABLE_NAME_SEISME]
    elif risk_layer == 'incendie':
        table_names = FIRE_TABLE_NAMES
        is_fire_risk = True 
    elif risk_layer == 'bee':
        table_names = [RISK_TABLE_NAME_BEE]
        
    elif risk_layer == 'seismeTa':
        table_names = [RISK_TABLE_NAME_SEISME_TA]

    
    elif risk_layer.startswith('mvt_terrain'):
        try:
            dept_num = risk_layer.split(':')[1]
            table_names = [f'"mvt_dptList_{dept_num}"']
        except IndexError:
            return jsonify({"error": f"Format MVT de terrain invalide. Attendu: mvt_terrain:XX"}), 400
    else:
        return jsonify({"error": f"Couche de risque non supportée: {risk_layer}"}), 400


    if not user_geojson_collection or user_geojson_collection.get('type') != 'FeatureCollection':
        return jsonify({"error": "Collection de géométries utilisateur GeoJSON (FeatureCollection) manquante ou invalide."}), 400


    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        user_geojson_str = json.dumps(user_geojson_collection)


        # 2. Construction de la requête avec support de multiples tables de risque (UNION ALL)
        risk_intersection_checks = []
        risk_intersection_fetch = []
        
        for i, table_name in enumerate(table_names):
            if is_fire_risk:
                risk_geom_expression = f'ST_SetSRID(ST_MakePoint(r{i}."XRan", r{i}."YRan"), 4326)'
                risk_fetch_geom_expression = f'ST_SetSRID(ST_MakePoint(t."XRan", t."YRan"), 4326)'
                where_clause = f'AND r{i}."XRan" IS NOT NULL AND r{i}."YRan" IS NOT NULL'
                fetch_where_clause = f'AND t."XRan" IS NOT NULL AND t."YRan" IS NOT NULL'
            else:
                risk_geom_expression = f'r{i}.geom'
                risk_fetch_geom_expression = f't.geom'
                where_clause = f'AND r{i}.geom IS NOT NULL'
                fetch_where_clause = f'AND t.geom IS NOT NULL'


            # Clause EXISTS: vérifie l'intersection avec CHAQUE table de risque
            risk_intersection_checks.append(f"""
                EXISTS (
                    SELECT 1
                    FROM public.{table_name} AS r{i}
                    WHERE ST_Intersects(ST_SetSRID(u.geom, 4326), {risk_geom_expression})
                    {where_clause}
                )
            """)
            
            # MISE À JOUR CRITIQUE: Retire la condition de jointure spatiale (WHERE ou JOIN) 
            # et sélectionne TOUTES les entités de la table de risque
            risk_intersection_fetch.append(f"""
                SELECT DISTINCT ON (t.ctid) -- Unicité par zone de risque (ctid est l'ID interne de la ligne)
                    -1 AS user_feature_id, -- ID factice
                    ST_AsGeoJSON({risk_fetch_geom_expression})::jsonb AS risk_feature_geojson,
                    '{risk_layer}'::text AS risk_layer_name,
                    t.*::text as original_props
                FROM public.{table_name} AS t
                -- WHERE [PAS DE WHERE SPATIAL ICI POUR INCLURE TOUT]
                WHERE {fetch_where_clause.replace('AND', '')} -- Conserve uniquement la clause NOT NULL
            """)


        is_in_risk_condition = " OR ".join(risk_intersection_checks)
        risk_union_query = " UNION ALL ".join(risk_intersection_fetch)


        query = f"""
            WITH
            individual_user_features AS (
                SELECT
                    ROW_NUMBER() OVER () AS feature_id,
                    ST_GeomFromGeoJSON(features.feat ->> 'geometry') AS geom,
                    features.feat -> 'properties' AS properties,
                    features.feat -> 'geometry' AS original_geom_json 
                FROM json_array_elements(%s::json -> 'features') AS features(feat)
                WHERE features.feat ->> 'geometry' IS NOT NULL AND ST_GeomFromGeoJSON(features.feat ->> 'geometry') IS NOT NULL
            ),


            user_collection AS (
                -- Collection agrégée de TOUTES les géométries utilisateur
                SELECT ST_Collect(geom) AS collective_geom FROM individual_user_features
            ),


            intersection_summary AS (
                SELECT
                    u.feature_id,
                    u.original_geom_json,
                    u.properties,
                    ( {is_in_risk_condition} ) AS is_in_risk_zone
                FROM individual_user_features u
            ),


            final_stats AS (
                SELECT
                    COUNT(*) AS total_features,
                    SUM(CASE WHEN is_in_risk_zone THEN 1 ELSE 0 END) AS features_in_risk,
                    (COUNT(*) - SUM(CASE WHEN is_in_risk_zone THEN 1 ELSE 0 END)) AS features_not_in_risk
                FROM intersection_summary
            ),
            
            -- Les zones de risque incluent maintenant TOUTES les entités sources
            intersected_risks AS (
                {risk_union_query}
            )


            -- 5. Construction du GeoJSON final (incluant user_location ET TOUTES les risk_zone)
            SELECT json_build_object(
                'type', 'FeatureCollection',
                'features', (
                    -- Agrégation de TOUS les risques (maintenant sans condition spatiale)
                    (
                        SELECT COALESCE(jsonb_agg(
                            jsonb_set(
                                jsonb_set(
                                    jsonb_set(
                                        jsonb_build_object('type', 'Feature', 'geometry', risk_feature_geojson -> 'geometry', 'properties', '{{}}'::jsonb),
                                        '{{properties,feature_type}}',
                                        '"risk_zone"'::jsonb,
                                        true
                                    ),
                                    '{{properties,risk_layer}}',
                                    to_jsonb(risk_layer_name),
                                    true
                                ),
                                '{{properties,original_props}}',
                                to_jsonb(
                                    CASE WHEN risk_layer_name = 'incendie'
                                    THEN jsonb_build_object('XRan', (risk_feature_geojson -> 'geometry' -> 'coordinates' -> 0), 'YRan', (risk_feature_geojson -> 'geometry' -> 'coordinates' -> 1))
                                    ELSE (risk_feature_geojson -> 'properties') 
                                    END
                                ),
                                true
                            )
                        )::jsonb, '[]'::jsonb)
                        FROM intersected_risks
                    )
                    ||
                    -- Agrégation des géométries utilisateur (en risque et hors risque)
                    (
                        SELECT COALESCE(jsonb_agg(
                            jsonb_set(
                                jsonb_set(
                                    jsonb_build_object(
                                        'type', 'Feature',
                                        'geometry', u.original_geom_json,
                                        'properties', u.properties
                                    ),
                                    '{{properties,feature_type}}',
                                    '"user_location"'::jsonb,
                                    true
                                ),
                                '{{properties,is_in_risk}}',
                                to_jsonb(u.is_in_risk_zone), 
                                true
                            )
                        )::jsonb, '[]'::jsonb)
                        FROM intersection_summary u
                    )
                ),
                'stats', (
                    SELECT json_build_object(
                        'total_features', s.total_features,
                        'features_in_risk', s.features_in_risk,
                        'features_not_in_risk', s.features_not_in_risk,
                        'percentage_in_risk', CASE
                            WHEN s.total_features > 0 THEN ROUND((s.features_in_risk * 100.0) / s.total_features, 2)
                            ELSE 0
                        END
                    ) FROM final_stats s
                )
            )
            FROM final_stats
            LIMIT 1;
        """
        
        cur.execute(query, (user_geojson_str,))
        geojson_result = cur.fetchone()[0]


        if geojson_result and geojson_result.get('features') is not None:
            geojson_result['features'] = [f for f in geojson_result['features'] if f is not None]
            return jsonify(geojson_result)
        else:
            empty_stats = {
                "total_features": 0, "features_in_risk": 0,
                "features_not_in_risk": 0, "percentage_in_risk": 0
            }
            return jsonify({"type": "FeatureCollection", "features": [], "stats": empty_stats})


    except psycopg2.Error as e:
        print(f"Erreur PostgreSQL (critique - Batch Intersect): {e}")
        traceback.print_exc()
        return jsonify({"error": f"Erreur SQL lors de l'intersection: {str(e)}"}), 500
    except Exception as e:
        print(f"Erreur inattendue: {e}")
        traceback.print_exc()
        return jsonify({"error": f"Erreur serveur inattendue: {str(e)}"}), 500
    finally:
        if conn: conn.close()


# ------------------------------------------------------------------


# =======================================================
# ROUTE MISE À JOUR : Chargement complet (supporte désormais 'incendie')
# =======================================================
@app.route('/api/risk-data/<layer_name>', methods=['GET'])
def get_risk_data_v2(layer_name):
    """
    Récupère le GeoJSON complet pour une couche donnée.
    Supporte 'inondation', 'seismes', 'incendie' et 'mvt_terrain:[numero_departement]'.
    """
    
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # 1. Détermination de la source de données
        union_query = None


        if layer_name == "inondation":
            query = f"SELECT json_build_object('type', 'FeatureCollection', 'features', json_agg(ST_AsGeoJSON(t.*)::json) ) FROM public.{RISK_TABLE_NAME_FLOOD} AS t WHERE t.geom IS NOT NULL"
            
        elif layer_name == "seismes":
            query = f"SELECT json_build_object('type', 'FeatureCollection', 'features', json_agg(ST_AsGeoJSON(t.*)::json) ) FROM public.{RISK_TABLE_NAME_SEISME} AS t WHERE t.geom IS NOT NULL"
        
        elif layer_name == "incendie":
            select_statements = []
            for table_name in FIRE_TABLE_NAMES:
                select_statements.append(f"""
                    (SELECT t.*, ST_SetSRID(ST_MakePoint(t."XRan", t."YRan"), 4326) AS geom FROM public.{table_name} AS t WHERE t."XRan" IS NOT NULL AND t."YRan" IS NOT NULL)
                """)
            
            union_query = " UNION ALL ".join(select_statements)
            query = f"""
                SELECT json_build_object('type', 'FeatureCollection',
                    'features', json_agg(ST_AsGeoJSON(t.*)::json)
                ) FROM ({union_query}) AS t
            """
        
        elif layer_name == "bee":
            query = f"SELECT json_build_object('type', 'FeatureCollection', 'features', json_agg(ST_AsGeoJSON(t.*)::json) ) FROM public.{RISK_TABLE_NAME_BEE} AS t WHERE t.geom IS NOT NULL"


        elif layer_name == "Seisme_Ta_1973_2025":
            query = f"SELECT json_build_object('type', 'FeatureCollection', 'features', json_agg(ST_AsGeoJSON(t.*)::json) ) FROM public.{RISK_TABLE_NAME_SEISME_TA} AS t WHERE t.geom IS NOT NULL"


        elif layer_name.startswith("mvt_terrain:"):
            dept_num = layer_name.split(':')[1]
            table_name = f'"mvt_dptList_{dept_num}"'
            query = f"SELECT json_build_object('type', 'FeatureCollection', 'features', json_agg(ST_AsGeoJSON(t.*)::json) ) FROM public.{table_name} AS t WHERE t.geom IS NOT NULL"
            
        else:
            return jsonify({"error": f"Couche de risque non supportée: {layer_name}"}), 404
            
        # 2. Exécution de la requête
        cur.execute(query)
        geojson_result = cur.fetchone()[0]
        
        # 3. Retour du résultat
        return jsonify(geojson_result or {"type": "FeatureCollection", "features": []})
        
    except psycopg2.Error as e:
        error_source = layer_name if not union_query else 'Union Incendie'
        print(f"Erreur PostgreSQL lors de la récupération de la couche {error_source}: {e}")
        traceback.print_exc()
        return jsonify({"error": f"Erreur de base de données lors du chargement de la couche {layer_name}: {str(e)}"}), 500
    except Exception as e:
        print(f"Erreur inattendue: {e}")
        traceback.print_exc()
        return jsonify({"error": f"Erreur serveur inattendue lors du chargement de la couche: {str(e)}"}), 500
    finally:
        if conn: conn.close()


# ------------------------------------------------------------------


if __name__ == '__main__':
    print("SERVEUR PYTHON INTERSECT LANCE (Batch Mode Activé avec Stats)")
    app.run(host='0.0.0.0', port=5000, debug=True)
