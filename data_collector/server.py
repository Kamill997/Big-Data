from concurrent import futures
from datetime import datetime
import os
import threading
import grpc
import asyncio
import time
from database import init_db
from flask import Flask, request, jsonify
import requests
import user_service_pb2
import user_service_pb2_grpc
import mysql.connector
import json
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_USER = "root"
DB_PASSWORD = "root"
DB_NAME = "data_db"

def connect_db():
    return mysql.connector.connect(
        host=DB_HOST,
        #port=3306,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

with open("credentials.json", "r") as file:
    credentials = json.load(file)
    CLIENT_ID = credentials["clientId"]
    CLIENT_SECRET = credentials["clientSecret"]
url_token= "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
# Dati da inviare come form
data = {
    "grant_type": "client_credentials",
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET
}
# POST request
response = requests.post(url_token, data)
# Controlla che la richiesta sia andata a buon fine
if response.status_code == 200:
    token = response.json().get("access_token")
    print("Il token è:", token)
else:
    print("Errore:", response.status_code, response.text)

header = {
    "Authorization": f"Bearer {token}"
}


async def verifica_email_grpc(email: str):
    try:
        async with grpc.aio.insecure_channel("container_user_manager:50051 ") as channel:
            stub = user_service_pb2_grpc.UserServiceStub(channel)
            richiesta = await stub.VerificaEmail(
                user_service_pb2.EmailDaVerificare(email=email)
            )
            return richiesta.esiste
    except grpc.RpcError as e:
        return False

class UserService(user_service_pb2_grpc.UserServiceServicer):
    def EliminaInteresse(self, request, context):
        self.db = connect_db()
        cursor = self.db.cursor()
        try:
            cursor.execute("DELETE FROM user_interest WHERE email=%s", (request.email,))
            self.db.commit()
            successo = True
        except Exception:
            successo = False
        finally:
            self.db.close()

        return user_service_pb2.EsitoEliminazione(
            successo=successo
        )
def serve():
    port = '50051'
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    user_service_pb2_grpc.add_UserServiceServicer_to_server(UserService(), server)
    server.add_insecure_port('[::]:'+port)
    server.start()
    server.wait_for_termination()

@app.route('/sottoscriviInteresse', methods=['POST'])
async def sottoscriviInteresse():
    data=request.json
    email=data.get("email")
    if not email:
        return jsonify({"success": False,"message": "Email mancante"}), 400
    esitoVerifica=await verifica_email_grpc(email)
    if esitoVerifica:
        interessi=data.get("interessi")
        if not interessi:
            return jsonify({"success": False,"message": "Interesse mancante"}), 400
        db=connect_db()
        cursor=db.cursor()
        risultato_operazione = []
        try:
            for interesse in interessi:
                cursor.execute("SELECT * FROM user_interest WHERE airport_code=%s and email=%s", (interesse,email))
                if cursor.fetchone() is None:
                    insert="""INSERT INTO user_interest (email, airport_code) VALUES (%s, %s)"""
                    cursor.execute(insert, (email, interesse))
                    risultato_operazione.append({"message":f"L'interesse {interesse} è stato registrato correttamente"})
                else:
                    risultato_operazione.append({"message":f"L'interesse {interesse} era stato già indicato"})
            db.commit()
            return jsonify({"Eisto operazione": risultato_operazione})
        except Exception as e:
            db.rollback()
            return jsonify({"error": f"Qualcosa è andato storto, interessi non registrati"}), 400
        finally:
            cursor.close()
            db.close()
    else:
        return jsonify({"error": f"L'email non è stata mai registrata"}), 422

@app.route('/eliminaInteresse',methods=['DELETE'])
async def eliminaInteresse():
    data=request.json
    email=data.get("email")
    if email is None:
        return jsonify({"Error":"Non hai indicato un email"}), 400
    esitoVerifica=await verifica_email_grpc(email)
    if esitoVerifica:
        interessi=data.get("interessi")
        if not interessi:
            return jsonify({"Error":"Non hai indicato un interesse"}), 400
        db=connect_db()
        cursor=db.cursor()
        risultato_operazione = []
        try:
            for interesse in interessi:
                cursor.execute("SELECT * FROM user_interest WHERE email=%s and airport_code=%s", (email,interesse))
                if cursor.fetchone():
                    cursor.execute("DELETE FROM user_interest WHERE email=%s AND airport_code=%s", (email,interesse))
                    risultato_operazione.append({"Message":f"L'interesse {interesse} è stato eliminato"})
                else:
                    risultato_operazione.append({"Message": f"L'interesse {interesse} non è presente"})
            db.commit()
            return jsonify({"Esito operazione": risultato_operazione})
        except Exception as e:
            db.rollback()
            return jsonify({"Error":f"Qualcosa è andato storto e gli interessi non sono stati eliminati"})
        finally:
            cursor.close()
            db.close()
    else:
        return jsonify({"Error":"Email non registrata"}),422

@app.route('/visualizzaInfo',methods=['POST'])
async def visualizzaInfo():
    data=request.json
    email=data.get("email")
    if email is None:
        return jsonify({"Error":"Non hai indicato un email"}), 400
    esitoVerifica=await verifica_email_grpc(email)
    if not esitoVerifica:
        return jsonify({"Error":"Email non registrata"}), 400
    icao=data.get("icao")
    if icao is None:
        return jsonify({"Error":"Non hai indicato un icao"}), 400
    db=connect_db()
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM user_interest where email=%s and airport_code=%s", (email,icao))
    if not cursor.fetchone():
        return jsonify({"Error":"Non hai registrato questo interesse"}), 400
    cursor.execute("SELECT * from flights where aereoporto_arrivo=%s",(icao,))
    results = cursor.fetchall()   # lista di dict
    if not results:
        return jsonify({"message": "Nessuna info trovata per questo ICAO"}), 404
    return jsonify({"flights": results}), 200


@app.route('/prova',methods=['GET'])
def download_flights_for_airport():
    db= connect_db()
    cursor = db.cursor()

    end_time = int(time.time())
    start_time=end_time-24*60*60

    cursor.execute("SELECT DISTINCT airport_code FROM user_interest")
    interessi = cursor.fetchall()

    if(not interessi):
        return jsonify({"Error":"Non ci sono interessi"}), 400
    airports = [icao[0] for icao in interessi]
    for airport in airports:
        parametri = {
            "airport": airport,
            "begin": start_time,
            "end": end_time,

        }

        url= "https://opensky-network.org/api/flights/arrival"
        response = requests.get(url, headers=header, params=parametri)
        if response.status_code != 200:
            print(f"Errore API per {airport}:", response.status_code)
            continue
        voli=response.json()
        if not voli:
            print(f"Volo vuoto")
        for volo in voli:
            print(f"Volo: {volo.get("estDepartureAirport")}")
            try:
                query="""INSERT INTO flights ( aereoporto_partenza, aereoporto_arrivo, icao_volo,orario_partenza,orario_arrivo)
                         VALUES (%s, %s, %s,%s,%s)"""
                icao24=volo.get("icao24")
                aereoporto_partenza=volo.get("estDepartureAirport")
                aereoporto_arrivo=volo.get("estArrivalAirport")
                orario_partenza=volo.get('firstSeen')
                orario_arrivo=volo.get('lastSeen')
                cursor.execute(query, (aereoporto_partenza, aereoporto_arrivo, icao24,orario_partenza,orario_arrivo))
                db.commit()
            except Exception as e:
                print("Errore durante l'inserimento delle informazioni del volo con icao:", icao24,e)
    cursor.close()
    db.close()
    return jsonify({"interessi_utente": airports}), 200



scheduler = BackgroundScheduler()
def start_scheduler():
    print("scheduler partito")
    scheduler.add_job(download_flights_for_airport, 'interval', minutes=1, next_run_time=datetime.now())


if __name__ == "__main__":
    init_db()
    scheduler_thread = threading.Thread(target=start_scheduler, daemon=True)
    scheduler_thread.start()
    grpc_thread = threading.Thread(target=serve, daemon=True)
    grpc_thread.start()
    app.run(host="0.0.0.0", port=5000, debug=True)
