import os
from flask import Flask, render_template, jsonify
from flask_pymongo import  PyMongo
from dotenv import load_dotenv

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
    



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
    
