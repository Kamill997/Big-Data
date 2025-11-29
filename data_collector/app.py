import os
import grpc
import schedule
import time
import threading
import asyncio
from database import init_db
from flask import Flask,request,jsonify
import user_service_pb2
import user_service_pb2_grpc
import mysql.connector
import requests
import json

app = Flask(__name__)

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_USER = "root"
DB_PASSWORD = "root"
DB_NAME = "data_db"

OPENSKY_API_URL = "https://opensky-network.org/api/flights"

CREDENTIALS_FILE = "/app/credentials.json"
try:
    if os.path.exists(CREDENTIALS_FILE):
        with open(CREDENTIALS_FILE, 'r') as f:
            creds = json.load(f)

            # --- MODIFICA QUI ---
            # Mappiamo il tuo "clientId" nello username
            # E il tuo "clientSecret" nella password
            OPENSKY_USER = creds.get("clientId")
            OPENSKY_PASS = creds.get("clientSecret")

            OPENSKY_USER = None  # Forza anonimo
            OPENSKY_PASS = None  # Forza anonimo
            print(f"[Config] Credenziali caricate: {OPENSKY_USER}")
    else:
        print(f"[Config] File {CREDENTIALS_FILE} non trovato. Uso modalità anonima.")
except Exception as e:
    print(f"[Config] Errore lettura credentials.json: {e}")

def connect_db():
    return mysql.connector.connect(
        host=DB_HOST,
        #port=3306,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )


# --- 1. CLIENT gRPC (SINCRONO) ---
#def verify_email_grpc(email: str):
   # try:
   #     # Usa insecure_channel normale (non aio/async)
   #     with grpc.insecure_channel("container_user_manager:50051") as channel:
   #         stub = user_service_pb2_grpc.UserServiceStub(channel)
   #         # Chiamiamo il metodo VerifyUser definito nel proto
  #          response = stub.VerifyUser(user_service_pb2.UserRequest(email=email))
 #           return response.exists
 #   except grpc.RpcError as e:
 #       print(f"[gRPC Error] {e}")
#        return False

def verify_email_grpc(email: str):

    try:
        with grpc.insecure_channel("container_user_manager:50051") as channel:
            stub = user_service_pb2_grpc.UserServiceStub(channel)
            requested = stub.VerifyUser(user_service_pb2.UserRequest(email=email))
            return requested.exists
    except grpc.RpcError as e:
        return False

#def airports_flights(airport_code,start_time,end_time):
 #   conn=connect_db()
  #  cursor=conn.cursor()

   # auth=(OPENSKY_USER,OPENSKY_PASS) if OPENSKY_USER and OPENSKY_PASS else None

def download_flights_for_airport(airport_code, start_time, end_time):
    """
    Funzione helper che scarica SIA arrivi CHE partenze per un singolo aeroporto
    e li salva nel DB. Viene chiamata sia dal ciclo sia dall'inserimento immediato.
    """
    conn = connect_db()
    cursor = conn.cursor()

    # Auth tuple per requests (se le credenziali ci sono)
    auth = (OPENSKY_USER, OPENSKY_PASS) if OPENSKY_USER and OPENSKY_PASS else None

    # Tipi di volo da scaricare
    endpoints = [
        ("arrival", True),   # (url_suffix, is_arrival_flag)
        ("departure", False)
    ]

    count_saved = 0

    for suffix, is_arrival in endpoints:
        url = f"{OPENSKY_API_URL}/{suffix}"
        params = {
            'airport': airport_code,
            'begin': start_time,
            'end': end_time
        }

        try:
            # --- DEBUG AGGIUNTO ---
            print(f"[DEBUG AUTH] User inviato: '{OPENSKY_USER}' | Pass inviata: '{OPENSKY_PASS}'")
            # --- DEBUG AGGIUNTO ---
            print(f"[OpenSky] Request {suffix.upper()} per {airport_code}...")
            response = requests.get(url, params=params, auth=auth, timeout=10)

            if response.status_code == 200:
                flights = response.json()

                print(f"[OpenSky] Trovati {len(flights)} voli ({suffix})")

                for flight in flights:
                    # Inserimento nel DB (Flight Data)
                    sql = """INSERT INTO flights
                             (interested_ICAO, icao_flight, origin_country, departure_time, arrival_time, is_arrival)
                             VALUES (%s, %s, %s, %s, %s, %s)"""
                    valori = (
                        airport_code,
                        flight.get('icao24'),
                        flight.get('originCountry') or "Unknown",
                        #   flight.get('estDepartureAirport') if is_arrival else flight.get('estArrivalAirport'),
                        flight.get('firstSeen'),
                        flight.get('lastSeen'),
                        is_arrival
                    )
                    cursor.execute(sql, valori)
                count_saved += len(flights)
            elif response.status_code == 404:
                print(f"[OpenSky] Nessun dato trovato per {airport_code} ({suffix})")
            else:
                print(f"[OpenSky] Errore {response.status_code}: {response.text}")

        except Exception as e:
            print(f"[OpenSky] Errore connessione: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print(f"[OpenSky] {airport_code}: Salvati {count_saved} voli totali (Arr+Dep).")

def fetch_opensky_data_cycle():
    """Task ciclico: Scarica i dati per TUTTI gli aeroporti nel DB"""
    print("--- [Ciclo] Inizio aggiornamento globale ---")
    conn = connect_db()
    cursor = conn.cursor()

    cursor.execute("SELECT DISTINCT airport_code FROM user_interest")
    airports = cursor.fetchall() # Ritorna lista di tuple [('LIRF',), ('EDDF',)]
    cursor.close()
    conn.close()

    # Intervallo temporale (Ultima ora)
    end_time = int(time.time())
    start_time = end_time - 3600

    for (airport_code,) in airports:
        download_flights_for_airport(airport_code, start_time, end_time)

    print("--- [Ciclo] Fine aggiornamento globale ---")

def run_scheduler():
    #schedule.every(12).hours.do(fetch_opensky_data_cycle)
    ## Per debug
    schedule.every(10).minutes.do(fetch_opensky_data_cycle)
    while True:
        schedule.run_pending()
        time.sleep(1)

#API
@app.post('/sottoscriviInteresse')
def sottoscriviInteresse():
    print("[DEBUG] Ricevuta richiesta sottoscrizione...") # DEBUG
    data = request.json
    email = data.get("email")

    if not email:
        return jsonify({"success": False, "message": "Email mancante"}), 400

    # 1. Verifica esistenza utente
    esitoVerifica = verify_email_grpc(email)

    if esitoVerifica:
        interessi = data.get("interessi")
        if not interessi:
            return jsonify({"success": False, "message": "Interesse mancante"}), 400

        if isinstance(interessi, str):
            interessi = [interessi]

        db = connect_db()
        cursor = db.cursor()
        risultato_operazione = []

        # Lista per tenere traccia di cosa scaricare subito
        da_scaricare_subito = []

        try:
            for interesse in interessi:
                # Nota: assicurati che i nomi delle colonne 'airport_code' e 'email'
                # corrispondano esattamente alla tua tabella user_interest
                # Nel tuo init_db avevi scritto: email, airport_code

                # QUERY DI CONTROLLO
                cursor.execute("SELECT * FROM user_interest WHERE airport_code=%s and email=%s", (interesse, email))

                if cursor.fetchone() is None:
                    # È NUOVO -> LO INSERISCO
                    insert = """INSERT INTO user_interest (email, airport_code) VALUES (%s, %s)"""
                    cursor.execute(insert, (email, interesse))
                    risultato_operazione.append({"message": f"L'interesse {interesse} è stato registrato correttamente"})

                    # AGGIUNGO ALLA LISTA DOWNLOAD
                    da_scaricare_subito.append(interesse)
                    print(f"[DEBUG] {interesse} è nuovo -> Aggiunto alla coda di download.")
                else:
                    # ESISTEVA GIÀ -> NON LO SCARICO
                    risultato_operazione.append({"message": f"L'interesse {interesse} era stato già indicato"})
                    print(f"[DEBUG] {interesse} esisteva già -> Niente download.")

            db.commit()

            # =================================================================
            # AVVIO DOWNLOAD IMMEDIATO
            # =================================================================
            if da_scaricare_subito:
                print(f"[TEST] Avvio thread per: {da_scaricare_subito}")

                end_t = int(time.time())
                start_t = end_t - 7200 # Ultime 2 ore

                def quick_fetch():
                    try:
                        print("[THREAD] Thread partito...")
                        for apt in da_scaricare_subito:
                            print(f"[THREAD] Chiamo download_flights per {apt}")
                            # Assicurati che download_flights_for_airport sia definita fuori!
                            download_flights_for_airport(apt, start_t, end_t)
                        print("[THREAD] Finito.")
                    except Exception as thread_e:
                        print(f"[THREAD ERROR] Errore nel thread: {thread_e}")

                thread = threading.Thread(target=quick_fetch)
                thread.daemon = True
                thread.start()
            else:
                print("[DEBUG] Nessun nuovo interesse da scaricare.")
            # =================================================================

            return jsonify({"Esito operazione": risultato_operazione})

        except Exception as e:
            db.rollback()
            print(f"[ERRORE SQL] {e}") # Stampa l'errore
            return jsonify({"error": f"Errore server: {str(e)}"}), 500
        finally:
            cursor.close()
            db.close()
    else:
        return jsonify({"error": f"L'email non è stata mai registrata"}), 422
@app.delete('/eliminaInteresse')
def eliminaInteresse():
    data=request.json
    email=data.get("email")
    if email is None:
        return jsonify({"Error":"Non hai indicato un email"}), 400
    esitoVerifica=verify_email_grpc(email)
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

if __name__ == "__main__":
    time.sleep(5)
    init_db()
    scheduler_thread = threading.Thread(target=run_scheduler)
    scheduler_thread.daemon = True # Si chiude quando chiudi l'app
    scheduler_thread.start()
    app.run(host="0.0.0.0", port=5000, debug=True)