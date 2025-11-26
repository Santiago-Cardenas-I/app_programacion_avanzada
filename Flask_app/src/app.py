import os
from flask import Flask, jsonify, request
from flask_pymongo import PyMongo
from dotenv import load_dotenv
from datetime import datetime
from dateutil import parser
from collections import defaultdict

# ----------------------------------------------------
# 1. CONFIGURACIÓN
# ----------------------------------------------------
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

MONGO_URI = os.getenv("MONGO_URI")

app = Flask(__name__)
app.config["MONGO_URI"] = MONGO_URI

# ----------------------------------------------------
# 2. CONEXIÓN A MONGODB
# ----------------------------------------------------
try:
    mongo = PyMongo(app)
    Sensor1_collection = mongo.db.Sensor1
    Sensor1_collection.find_one()
    print("Conectado a MongoDB Atlas")
except Exception as e:
    print("Error Mongo:", e)
    mongo = None
    Sensor1_collection = None

# ----------------------------------------------------
# POST → ESP32 ENVÍA DATOS AQUÍ
# ----------------------------------------------------
@app.route('/receive_sensor_data', methods=['POST'])
def receive_sensor_data():
    if Sensor1_collection is None:
        return jsonify({"error": "BD no disponible"}), 503

    try:
        data = request.get_json()

        if not data:
            return jsonify({"error": "No JSON recibido"}), 400

        sensor_type = data.get("sensor_type")
        value = data.get("value")
        unit = data.get("unit", "")

        if sensor_type is None or value is None:
            return jsonify({"error": "Campos incompletos"}), 400

        doc = {
            "sensor": sensor_type,
            "valor": value,
            "unidad": unit,
            "timestamp": datetime.now()
        }

        result = Sensor1_collection.insert_one(doc)

        return jsonify({
            "status": "success",
            "id": str(result.inserted_id),
            "saved": doc
        }), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ----------------------------------------------------
# GET /data?sensor=Temperature
# ----------------------------------------------------
@app.route('/data', methods=['GET'])
def get_sensor_data():
    sensor_type = request.args.get("sensor", "").strip()

    if not sensor_type:
        return jsonify({"error": "Debe usar ?sensor=XXXX"}), 400

    datos = list(Sensor1_collection.find(
        {"sensor": sensor_type},
        {"_id": 0}
    ))

    return jsonify(datos), 200


# ----------------------------------------------------
# ENDPOINTS PARA GRAFANA (Simple JSON Plugin)
# ----------------------------------------------------
@app.route('/')
def root():
    return "OK", 200


# ----------------------------------------------------
# /search → devuelve TODOS los sensores detectados
# ----------------------------------------------------
@app.route('/search', methods=['POST'])
def search_metrics():

    try:
        # Obtiene lista única de sensores registrados en la BD
        sensores = Sensor1_collection.distinct("sensor")
    except Exception as e:
        return jsonify({"error": f"No se pudieron obtener los sensores: {str(e)}"}), 500

    return jsonify(sensores), 200


# ----------------------------------------------------
# /query → devuelve los valores para el sensor consultado
# ----------------------------------------------------
@app.route('/query', methods=['POST'])
def query_data():
    req_data = request.get_json()

    try:
        t_from = parser.parse(req_data['range']['from'])
        t_to = parser.parse(req_data['range']['to'])
        targets = req_data['targets']
    except:
        return jsonify({"error": "Solicitud inválida"}), 400

    response = []

    for target in targets:
        metric = target['target']
        datapoints = []

        # Filtrar datos por rango de tiempo y por sensor
        cursor = Sensor1_collection.find(
            {
                "timestamp": {"$gte": t_from, "$lte": t_to},
                "sensor": metric
            },
            {"valor": 1, "timestamp": 1, "_id": 0}
        ).sort("timestamp", 1)

        # Convertir valores a formato Grafana
        for doc in cursor:
            try:
                ts = int(doc["timestamp"].timestamp() * 1000)
                datapoints.append([float(doc["valor"]), ts])
            except:
                continue

        response.append({
            "target": metric,
            "datapoints": datapoints
        })

    return jsonify(response)



# ----------------------------------------------------
# JSON PARA GRAFANA JSON API
# ----------------------------------------------------
@app.route('/json_api_data', methods=['GET'])
def json_api_data():
    cursor = Sensor1_collection.find(
        {},
        {"sensor": 1, "valor": 1, "timestamp": 1, "_id": 0}
    ).sort("timestamp", 1)

    grouped = defaultdict(list)

    for doc in cursor:
        grouped[doc["sensor"]].append({
            "time": doc["timestamp"].isoformat(),
            "value": float(doc["valor"])
        })

    return jsonify(grouped)


# ----------------------------------------------------
# RUN
# ----------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
