import os
from flask import Flask, render_template, jsonify, request
from flask_pymongo import  PyMongo
from dotenv import load_dotenv
from datetime import datetime 


dotenv_path = os.path.join(os.path.dirname(__file__), '.env')

load_dotenv(dotenv_path)

MONGO_HOST= os.getenv("MONGO_HOST", "mogli")
MONGO_PORT=os.getenv("MONGO_PORT", "8081")
MONGO_DB=os.getenv("MONGO_DB", " ")
MONGO_USER=os.getenv("MONGO_USER", " ")
MONGO_PASSWORD=os.getenv("MONGO_PASSWO", " ") 
MONGO_URI =os.getenv('MONGO_URI')

app = Flask(__name__)
app.config["MONGO_URI"] = MONGO_URI


try:
    mongo = PyMongo(app)
    
    Sensor1_collection = mongo.db.Sensor1
    print("Conexión a MongoDB y colección 'Sensor1' establecida.")

    Sensor1_collection.find_one()
    print("Prueba de lectura a la colección 'Sensor1' exitosa.")
except Exception as e:
    print(f"Error al conectar o interactuar con MongoDB: {e}")
    mongo = None
    Sensor1_collection = None


@app.route('/')
def index():
    return "Hello, World!"

@app.route('/archivo')
def archivo():
    return render_template('archivo.html')

@app.route('/insert')
def insert_data():
    if Sensor1_collection is not None:
        try:
            
            dato_Sensor1 = {"sensor": "temperatura_prueba", "valor": 2.1, "unidad": "C"}
            # Insertamos el dato en la colección 'Sensor1'
            result = Sensor1_collection.insert_one(dato_Sensor1)
            return jsonify({
                "mensaje": "Dato de prueba agregado exitosamente a 'Sensor1'",
                "id": str(result.inserted_id)
            })
        except Exception as e:
            return jsonify({"error": f"Error al insertar en la base de datos: {e}"}), 500
    else:
        return jsonify({"error": "La conexión a la base de datos no está establecida."}), 500
    
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



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
    
