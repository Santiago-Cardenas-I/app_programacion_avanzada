import os
from flask import Flask, jsonify, request
from flask_pymongo import PyMongo
from dotenv import load_dotenv
from datetime import datetime
from dateutil import parser
from collections import defaultdict

# ----------------------------------------------------
# 1. CONFIGURACIÓN INICIAL
# ----------------------------------------------------
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

# URI Mongo
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin123@mongo:27017/proyecto_db?authSource=admin")

app = Flask(__name__)
app.config["MONGO_URI"] = MONGO_URI

# ----------------------------------------------------
# 2. CONEXIÓN A MONGODB
# ----------------------------------------------------
try:
    mongo = PyMongo(app)
    Sensor1_collection = mongo.db.Sensor1
    print("✅ Conexión a MongoDB establecida.")
    Sensor1_collection.find_one()
except Exception as e:
    print(f"❌ Error al conectar: {e}")
    mongo = None
    Sensor1_collection = None


# ----------------------------------------------------
# POST /receive_sensor_data (ESP32)
# ----------------------------------------------------
@app.route('/receive_sensor_data', methods=['POST'])
def receive_sensor_data():
    if Sensor1_collection is None:
        return jsonify({"error": "BD no disponible"}), 503

    try:
        data = request.get_json()

        if not data:
            return jsonify({"error": "No JSON recibido"}), 400

        sensor_type = data.get('sensor_type')
        value = data.get('value')
        unit = data.get('unit', 'N/A')

        if sensor_type is None or value is None:
            return jsonify({"error": "Faltan campos"}), 400

        doc = {
            "sensor": sensor_type,
            "valor": value,
            "unidad": unit,
            "timestamp": datetime.now()
        }

        result = Sensor1_collection.insert_one(doc)

        return jsonify({
            "status": "success",
            "id_mongo": str(result.inserted_id),
            "data_received": doc
        }), 201

    except Exception as e:
        print("Error:", e)
        return jsonify({"error": str(e)}), 500


# ----------------------------------------------------
# GET /insert (prueba)
# ----------------------------------------------------
@app.route('/insert', methods=['GET'])
def insert_data():
    if Sensor1_collection is None:
        return jsonify({"error": "BD no disponible"}), 503

    dato = {
        "sensor": "Temperature_Test",
        "valor": 20.9,
        "unidad": "C",
        "timestamp": datetime.now()
    }
    result = Sensor1_collection.insert_one(dato)
    return jsonify({"mensaje": "Agregado", "id": str(result.inserted_id)}), 201


# ----------------------------------------------------
# GET /data?sensor=Temperature
# ----------------------------------------------------
@app.route('/data', methods=['GET'])
def get_sensor_data():
    sensor_type = request.args.get("sensor", "").strip()

    if not sensor_type:
        return jsonify({"error": "Debe usar ?sensor=Temperature"}), 400

    if Sensor1_collection is None:
        return jsonify({"error": "BD no disponible"}), 503

    datos = list(Sensor1_collection.find(
        {"sensor": sensor_type},
        {"_id": 0}
    ))

    return jsonify(datos), 200


# ----------------------------------------------------
# ENDPOINTS GRAFANA SIMPLE JSON
# ----------------------------------------------------
@app.route('/', methods=['GET'])
def root_path():
    return 'OK', 200


@app.route('/search', methods=['POST'])
def search_metrics():
    # 🔹 MÉTRICAS DISPONIBLES
    metrics = [
        "Temperature",
        "Humidity",
        "MQ135_raw",
        "Air_quality"
    ]
    return jsonify(metrics)


@app.route('/query', methods=['POST'])
def query_data():
    if Sensor1_collection is None:
        return jsonify({"error": "BD no disponible"}), 503

    req_data = request.get_json(silent=True)

    if not req_data or 'range' not in req_data or 'targets' not in req_data:
        return jsonify({"error": "Solicitud inválida"}), 400

    try:
        time_from = parser.parse(req_data['range']['from'])
        time_to = parser.parse(req_data['range']['to'])
        targets = req_data['targets']
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    response_data = []

    for target_info in targets:
        metric_name = target_info['target']
        datapoints = []

        query_filter = {
            "timestamp": {'$gte': time_from, '$lte': time_to},
            "sensor": metric_name
        }

        projection = {"valor": 1, "timestamp": 1, "_id": 0}

        cursor = Sensor1_collection.find(query_filter, projection).sort("timestamp", 1)

        for doc in cursor:
            try:
                value = float(doc['valor'])
                ts = int(doc['timestamp'].timestamp() * 1000)
                datapoints.append([value, ts])
            except:
                continue

        response_data.append({
            "target": metric_name,
            "datapoints": datapoints
        })

    return jsonify(response_data)


# ----------------------------------------------------
# JSON API PARA GRAFANA PLUGIN
# ----------------------------------------------------
@app.route('/json_api_data', methods=['GET', 'POST'])
def json_api_data():
    if Sensor1_collection is None:
        return jsonify({"error": "BD no disponible"}), 503

    try:
        cursor = Sensor1_collection.find(
            {},
            {'sensor': 1, 'valor': 1, 'timestamp': 1, '_id': 0}
        ).sort("timestamp", 1)

        grouped_data = defaultdict(list)

        for doc in cursor:
            sensor = doc.get('sensor')
            valor = doc.get('valor')
            time_str = doc.get('timestamp').isoformat()

            grouped_data[sensor].append({
                "time": time_str,
                "value": float(valor)
            })

        return jsonify({
            "Temperature_data": grouped_data["Temperature"],
            "Humidity_data": grouped_data["Humidity"],
            "MQ135_raw_data": grouped_data["MQ135_raw"],
            "Air_quality_data": grouped_data["Air_quality"]
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ----------------------------------------------------
# RUN
# ----------------------------------------------------
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
