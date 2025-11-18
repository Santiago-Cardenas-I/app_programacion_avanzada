import os
from flask import Flask, jsonify, request
from flask_pymongo import PyMongo
from dotenv import load_dotenv
from datetime import datetime
from dateutil import parser # Necesario para convertir el tiempo de Grafana (ISO 8601)
# from bson.json_util import dumps # No es estrictamente necesario si usamos jsonify
from collections import defaultdict

# ----------------------------------------------------
# 1. CONFIGURACIÓN INICIAL
# ----------------------------------------------------
# Carga las variables de entorno del archivo .env
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

# Obtiene la URI de MongoDB (Asumiendo que has actualizado la URI de Atlas en tu .env)
# Se mantiene la URI de fallback local/docker por seguridad si .env falla, pero
# recuerda que para Atlas necesitas la cadena mongodb+srv://...
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin123@mongo:27017/proyecto_db?authSource=admin")

app = Flask(__name__)
app.config["MONGO_URI"] = MONGO_URI

# ----------------------------------------------------
# 2. CONEXIÓN A MONGODB
# ----------------------------------------------------
try:
    mongo = PyMongo(app)
    # Accede a la colección Sensor1 dentro de la base de datos especificada en MONGO_URI
    Sensor1_collection = mongo.db.Sensor1
    print("✅ Conexión a MongoDB establecida.")
    # Prueba de conexión leyendo un solo registro
    Sensor1_collection.find_one() 
except Exception as e:
    print(f"❌ Error al conectar con MongoDB: {e}")
    mongo = None
    Sensor1_collection = None



# -----------------------------------------
# 🔹 POST /insert  → Recibe datos del ESP32
# -----------------------------------------
@app.route('/receive_sensor_data', methods=['POST'])
def receive_sensor_data():
    if Sensor1_collection is None:
        
        return jsonify({"error": "La conexión a la base de datos no está establecida."}), 503

    try:
        # Obtener los datos JSON
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "No se proporcionó un payload JSON"}), 400

        
        sensor_type = data.get('sensor_type')
        value = data.get('value')
        unit = data.get('unit', 'N/A') 

        if sensor_type is None or value is None:
            return jsonify({"error": "Faltan campos obligatorios: 'sensor_type' o 'value'"}), 400

        
        doc_to_insert = {
            "sensor": sensor_type,
            "valor": value,
            "unidad": unit,
            "timestamp": datetime.now() 
        }

        
        result = Sensor1_collection.insert_one(doc_to_insert)


        return jsonify({
            "status": "success",
            "message": "Dato de sensor recibido y guardado exitosamente.",
            "id_mongo": str(result.inserted_id),
            "data_received": doc_to_insert
        }), 201
    except Exception as e:
        print(f"Error al procesar los datos del sensor: {e}")
        return jsonify({"status": "error", "message": f"Error interno del servidor: {e}"}), 500



@app.route('/insert', methods=['GET'])
def insert_data():
    """Inserta un registro de prueba."""
    if Sensor1_collection is None:
        return jsonify({"error": "No hay conexión a la base de datos"}), 503

    dato = {
        # Usamos nombres exactos para probar: Temperature y Humidity
        "sensor": "Temperature_Test", 
        "valor": 20.9,
        "unidad": "C",
        "timestamp": datetime.now()
    }
    result = Sensor1_collection.insert_one(dato)
    return jsonify({"mensaje": "Dato agregado", "id": str(result.inserted_id)}), 201


# ---------------------------------------------------
# 🔹 GET /data?sensor=Temperature  → Datos para Grafana
# ---------------------------------------------------
@app.route('/data', methods=['GET'])
def get_sensor_data():

    sensor_type = request.args.get("sensor", "").strip().capitalize()

    if not sensor_type:
        return jsonify({"error": "Debe enviar ?sensor=Temperature o Humidity"}), 400

    if Sensor1_collection is None:
        return jsonify({"error": "No hay conexión a MongoDB"}), 503

    datos = list(Sensor1_collection.find(
        {"sensor": sensor_type},
        {"_id": 0}
    ))

    return jsonify(datos), 200

# ----------------------------------------------------
# 4. ENDPOINTS PARA GRAFANA (SIMPLE JSON API)
# ----------------------------------------------------

# 🔹 1. Endpoint de Estado (/)
@app.route('/', methods=['GET'])
def root_path():
    """Ruta usada por Grafana para probar conexión."""
    return 'OK', 200

# 🔹 2. Endpoint de Búsqueda (/search)
@app.route('/search', methods=['POST'])
def search_metrics():
    """Retorna la lista de métricas (valores del campo 'sensor') disponibles."""
    # Los nombres DEBEN COINCIDIR EXACTAMENTE con el valor del campo 'sensor' en tu DB
    metrics = ["Temperature", "Humidity"] 
    return jsonify(metrics)

# 🔹 3. Endpoint de Consulta (/query)
@app.route('/query', methods=['POST'])
def query_data():
    """Consulta MongoDB dentro del rango de tiempo y filtra por el nombre del sensor."""
    if Sensor1_collection is None:
        return jsonify({"error": "La conexión a MongoDB no está disponible."}), 503

    req_data = request.get_json(silent=True)
    
    # 1. Verificación de solicitud y extracción de targets/range
    if not req_data or 'range' not in req_data or 'targets' not in req_data:
        return jsonify({"error": "Solicitud JSON inválida o incompleta."}), 400

    try:
        time_from_str = req_data['range']['from']
        time_to_str = req_data['range']['to']
        targets = req_data['targets']
        
        # Parsear las cadenas de tiempo
        time_from = parser.parse(time_from_str)
        time_to = parser.parse(time_to_str)
    except Exception as e:
        # Captura errores si Grafana envía un formato de fecha incorrecto
        print(f"Error al procesar JSON de Grafana (Fechas/Claves): {e}")
        return jsonify({"error": f"Error en el formato de solicitud: {e}"}), 400

    response_data = []

    # 2. Iterar sobre las métricas solicitadas
    for target_info in targets:
        metric_name = target_info['target']
        datapoints = []
        
        # 3. Consulta MongoDB (Filtrando por tiempo Y por el valor del campo 'sensor')
        query_by_metric = {
            'timestamp': {'$gte': time_from, '$lte': time_to},
            'sensor': metric_name 
        }
        
        # Proyección: Solo necesitamos 'valor' y 'timestamp'
        projection_by_metric = {'valor': 1, 'timestamp': 1, '_id': 0}
        
        cursor = Sensor1_collection.find(query_by_metric, projection_by_metric).sort("timestamp", 1)

        # 4. Formateo de los datos
        for doc in cursor:
            try:
                # Accede al campo 'valor'
                value = doc.get('valor')
                timestamp_obj = doc.get('timestamp')
                
                # Conversión de tipos y formato
                if value is not None and timestamp_obj:
                    # Convertir el valor a float si es necesario
                    value = float(value) 
                    # Convertir datetime a milisegundos desde la época (EPOCH)
                    timestamp_ms = int(timestamp_obj.timestamp() * 1000)
                    
                    datapoints.append([value, timestamp_ms])
            except Exception as inner_e:
                print(f"⚠️ Documento de DB con error de tipo (valor o timestamp): {inner_e}")
                continue # Saltar este documento y seguir con el siguiente

        # 5. Agregar la serie al JSON de respuesta
        response_data.append({
            "target": metric_name,
            "datapoints": datapoints
        })

    return jsonify(response_data)


# app.py - Nuevo Endpoint

# app.py - Función json_api_data modificada para JSON API
from collections import defaultdict

@app.route('/json_api_data', methods=['GET', 'POST'])
def json_api_data():
    if Sensor1_collection is None:
        return jsonify({"error": "La conexión a MongoDB no está disponible."}), 503
    
    try:
        # Traer los últimos registros (puedes ajustar el límite o el rango de tiempo si es necesario)
        cursor = Sensor1_collection.find(
            {}, 
            {'sensor': 1, 'valor': 1, 'timestamp': 1, '_id': 0}
        ).sort("timestamp", 1) # Ordenamos por tiempo ascendente

        # Usamos defaultdict para agrupar los datos
        grouped_data = defaultdict(list)
        
        for doc in cursor:
            sensor_type = doc.get('sensor')
            
            if sensor_type in ["Temperature", "Humidity"]:
                value = float(doc.get('valor', 0)) 
                
                # Convertir datetime a string ISO 8601 (necesario para el plugin JSON API)
                time_str = doc.get('timestamp').isoformat() if doc.get('timestamp') else None
                
                # Agregamos el punto a la lista del sensor correspondiente
                grouped_data[sensor_type].append({
                    "time": time_str,
                    "value": value
                })

        # Devolvemos los datos separados por clave
        return jsonify({
            "Temperature_data": grouped_data["Temperature"],
            "Humidity_data": grouped_data["Humidity"]
        })

    except Exception as e:
        print(f"Error en el endpoint JSON API: {e}")
        return jsonify({"error": str(e)}), 500
    
    
    

    
if __name__ == '__main__':
    # Usar host='0.0.0.0' es crucial si se ejecuta dentro de un contenedor o para acceso externo
    app.run(host='0.0.0.0', port=5000)