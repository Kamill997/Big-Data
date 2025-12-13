import schedule
import time
import threading
from database import init_db
from flask import Flask,request,jsonify
from gRPC_Logic import serve
from dataCollector_logic import DataCollectorLogic
from OpenSky import run_scheduler

app = Flask(__name__)

#API
@app.post('/subscribeInterest')
async def subscribeInterest():

    data = request.json
    email = data.get("email")
    interessi = data.get("interessi")
    high = data.get("high_value")
    low = data.get("low_value")

    if not email: return jsonify({"error": "Email mancante"}), 400

    success, result, status = await DataCollectorLogic.subscribeInterest(email, interessi,high,low)

    if success:
        return jsonify({"Esito operazione": result}), status
    else:
        return jsonify({"error": result}), status

@app.delete('/removeInterest')
async def removeInterest():
    data = request.json
    email = data.get("email")
    interessi = data.get("interessi")

    if not email: return jsonify({"Error": "Email mancante"}), 400

    success, result, status = await DataCollectorLogic.removeInterest(email, interessi)

    if success:
        return jsonify({"Esito operazione": result}), status
    else:
        return jsonify({"Error": result}), status


@app.get('/flights/info')
async def showInfo():
    email = request.headers.get("email")
    icao = request.args.get("icao")

    if not email:
        return jsonify({"Error": "Email mancante in header"}), 400

    if not icao:
        return jsonify({"Error": "Icao mancante"}), 400

    success, result, status = await DataCollectorLogic.showInfo(email, icao)

    if success:
        return jsonify(result), status
    else:
        return jsonify({"message": result}), status

@app.get('/flights/last')
async def last_flights():

    email = request.headers.get("email")
    airport = request.args.get('airport')
    flight_type = request.args.get('type', 'departure')

    if not email:
        return jsonify({"error": "Inserire email"}), 400

    if not airport:
        return jsonify({"error": "Codice Aeroporto mancante"}), 400

    if flight_type not in ['departure', 'arrival']:
        return jsonify({"error": "Tipo non valido, type deve assumere solo il valore departure o arrival"}), 400

    success, result, status = await DataCollectorLogic.last_flights(email, airport, flight_type)

    if success:
        return jsonify(result), status
    else:
        return jsonify({"message": result}), status

@app.get('/flights/average')
async def average_flights():
    email = request.headers.get('email')
    airport = request.args.get('airport')
    if not email:
        return jsonify({"error": "Inserire email"}), 400
    try:
        days = int(request.args.get('days'))
    except:
        return jsonify({"error": "days deve essere un numero intero"}), 400

    flight_type = request.args.get('type', 'departure')

    if not airport:
        return jsonify({"error": "Airport mancante"}), 400

    if not days:
        return jsonify({"error": "Giorni mancanti"}), 400

    if flight_type not in ['departure', 'arrival']:
        return jsonify({"error": "Tipo non valido, type deve assumere solo il valore departure o arrival"}), 400

    success, result, status = await DataCollectorLogic.average_flights(email, airport, days, flight_type)

    if success:
        return jsonify(result), status
    else:
        return jsonify({"error": result}), status



if __name__ == "__main__":
    time.sleep(5)
    init_db()
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    grpc_thread = threading.Thread(target=serve, daemon=True)
    grpc_thread.start()
    app.run(host="0.0.0.0", port=5000, debug=True)