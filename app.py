from flask import Flask
from flask import jsonify,request
import pymysql 
#from flask_mysqldb import MySQL

app = Flask(__name__)

@app.route('/',methods=['GET'])
def index():
    return jsonify({'Mensaje':'Bienvenida',
                    'Metodo 1': '/Metadata  Para obtener los registros de las estaciones metereologicas',
                    'Metodo 2': '/Metadata/<estado>  Para obtener los registros de las estaciones metereologicas por estado',
                    'Metodo 3': '/Data/<lat>/<lon>  Para obtener los datos climaticos de la zona especificada por las coordenadas',
                    })

app.config['MYSQL_HOST'] = 'investigacion-topologia.cv8dbxpuxjm5.us-east-2.rds.amazonaws.com'
app.config['MYSQL_USER'] = 'admin'
app.config['MYSQL_PASSWORD'] = 'dinamita'
app.config['MYSQL_DB'] = 'EstacionesClimatologicas_SMN'

#mysql = pymysql(app)

mysql = pymysql.connect(
    host=app.config['MYSQL_HOST'],
    user=app.config['MYSQL_USER'],
    password=app.config['MYSQL_PASSWORD'],
    db=app.config['MYSQL_DB'],
)

@app.route('/api', methods=['GET'])
def get_data():
    try:
        limit_num = 100
        limit = min(int(request.args.get('limit', limit_num)), limit_num)  # Ajustar limite de visualizaciones
        cur = mysql.cursor()
        cur.execute(f"SELECT * FROM  EstacionesClimatologicas_SMN.Metadatos LIMIT {limit}")
        data = cur.fetchall()
        cur.close()

        results = []
        for row in data:
            serialized_row ={
                'id' : row[0],
                'clave_estacion' : row[1],
                'nombre' : row[2],
                'estado' : row[3],
                'municipio' : row[4],
                'situacion' : row[5],
                'latitud' : row[6],
                'longitud' : row[7],
                'altitud' : row[8],
                'emision_Fecha' : row[9]
            }
            results.append(serialized_row)

        return jsonify(results)

    except Exception as e:
        return jsonify({'error' : str(e)}), 500
    
@app.route("/api/<estado>",methods=["GET"])
def get_data_estado(estado):
    try:
        municipio = request.args.get("municipio")
        limit_num = 100
        limit = min(int(request.args.get('limit', limit_num)), limit_num)  # Ajustar limite de visualizaciones
        cur = mysql.cursor()

        if municipio:
            cur.execute(f"SELECT * FROM  EstacionesClimatologicas_SMN.Metadatos WHERE estado = %s AND municipio = %s LIMIT %s",(estado,municipio,limit))
        else:
            cur.execute(f"SELECT * FROM  EstacionesClimatologicas_SMN.Metadatos WHERE estado = %s LIMIT %s",(estado,limit))

        data = cur.fetchall()
        cur.close()

        results = []
        for row in data:
            serialized_row ={
                'id' : row[0],
                'clave_estacion' : row[1],
                'nombre' : row[2],
                'estado' : row[3],
                'municipio' : row[4],
                'situacion' : row[5],
                'latitud' : row[6],
                'longitud' : row[7],
                'altitud' : row[8],
                'emision_Fecha' : row[9]
            }
            results.append(serialized_row)

        return jsonify(results)

    except Exception as e:
        return jsonify({'error' : str(e)}), 500

@app.route('/api/<string:lat>/<string:lon>', methods=['GET'])
def get_data_coordenadas(lat, lon):
    try:
        cur = mysql.cursor()
        cur.execute("SELECT * FROM EstacionesClimatologicas_SMN.Metadatos WHERE CAST(latitud AS DECIMAL(5, 3)) = '{0}' AND CAST(longitud AS DECIMAL(6,3)) = '{1}'".format(lat,lon))
        data = cur.fetchall()
        cur.close()

        if not data:
            return jsonify({'error': 'No data found for the specified coordinates'})

        location = []
        for row in data:
            serialized_row ={
                'id' : row[0],
                'clave_estacion' : row[1],
                'nombre' : row[2],
                'estado' : row[3],
                'municipio' : row[4],
                'situacion' : row[5],
                'latitud' : row[6],
                'longitud' : row[7],
                'altitud' : row[8],
                'emision_Fecha' : row[9]
            }
            location.append(serialized_row)
            
        id_location = location[0]["id"]
        cur = mysql.cursor()
        cur.execute("SELECT * FROM EstacionesClimatologicas_SMN.DatosClimatologicos WHERE metadato_id = '{0}' LIMIT 1000".format(id_location))
        data_clima = cur.fetchall()
        cur.close()

        results = []
        for row in data_clima:
            serialized_row ={
                'id' : row[0],
                'metadato_id' : row[1],
                'fecha' : row[2],
                'precipitacion' : row[3],
                'evaporacion' : row[4],
                'tempMax' : row[5],
                'tempMin' : row[6],
            }
            results.append(serialized_row)

        return jsonify(results)

    except Exception as e:
        return jsonify({'error' : str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True) # Se sigue compilando a medida que se actualiza el codigo