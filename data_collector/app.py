import logging
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
OPENSKY_TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

CREDENTIALS_FILE = "/app/credentials.json"

CLIENT_ID = None
CLIENT_SECRET = None

try:
    if os.path.exists(CREDENTIALS_FILE):
        with open(CREDENTIALS_FILE, 'r') as f:
            creds = json.load(f)
            # Mappiamo le chiavi come nel tuo codice originale
            CLIENT_ID = creds.get("clientId")
            CLIENT_SECRET = creds.get("clientSecret")

            if CLIENT_ID and CLIENT_SECRET:
                logging.info(f"Client ID caricato: {CLIENT_ID}")
            else:
                logging.warning("File credenziali presente ma clientId o clientSecret mancanti.")
    else:
        logging.warning("File credenziali non trovato.")
except Exception as e:
    logging.error(f"Errore lettura credenziali: {e}")

def connect_db():
    return mysql.connector.connect(
        host=DB_HOST,
        #port=3306,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

def verify_email_grpc(email: str):

    try:
        with grpc.insecure_channel("container_user_manager:50051") as channel:
            stub = user_service_pb2_grpc.UserServiceStub(channel)
            requested = stub.VerifyUser(user_service_pb2.UserRequest(email=email))
            return requested.exists
    except grpc.RpcError as e:
        return False

# --- HELPER AUTENTICAZIONE (La tua logica isolata) ---
def get_opensky_headers():
    """
    Effettua la chiamata POST per ottenere il Bearer Token.
    Restituisce il dizionario headers o None se fallisce.
    """
    if not CLIENT_ID or not CLIENT_SECRET:
        logging.warning("[AUTH] Credenziali mancanti, procedo in modalità anonima.")
        return None

    logging.info(f"[AUTH DEBUG] Sto usando CLIENT_ID='{CLIENT_ID}'")

    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }

    try:
        logging.info("[AUTH DEBUG] Invio richiesta token a OpenSky...")
        # Chiamata per ottenere il token
        response = requests.post(OPENSKY_TOKEN_URL, data=data, timeout=10)

        logging.info(f"[AUTH DEBUG] Status Token Response: {response.status_code}")
        logging.info(f"[AUTH DEBUG] Body Token Response: {response.text[:200]}...")

        if response.status_code == 200:
            token = response.json().get("access_token")
            # logging.info(f"[AUTH] Token ottenuto con successo: {token[:10]}...") # Log parziale per sicurezza
            return {"Authorization": f"Bearer {token}"}
        else:
            logging.error(f"[AUTH] Errore richiesta token: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logging.error(f"[AUTH] Eccezione richiesta token: {e}")
        return None

def airports_flights(airport_code, start_time, end_time):
    """
    Funzione helper che scarica SIA arrivi CHE partenze per un singolo aeroporto
    e li salva nel DB. Viene chiamata sia dal ciclo sia dall'inserimento immediato.
    """
    print(f"[DEBUG] Inizio download per aeroporto {airport_code}")

    conn = connect_db()
    cursor = conn.cursor()

    headers = get_opensky_headers()

    if headers:
        print(f"[AUTH DEBUG] Sto usando Bearer Token negli headers.")
    else:
        print(f"[AUTH DEBUG] Nessun token, sto chiamando OpenSky in modalità ANONIMA.")
    # Auth tuple per requests (se le credenziali ci sono)
# = (OPENSKY_USER, OPENSKY_PASS) if OPENSKY_USER and OPENSKY_PASS else None

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
            #print(f"[DEBUG AUTH] User inviato: '{OPENSKY_USER}' | Pass inviata: '{OPENSKY_PASS}'")
            # --- DEBUG AGGIUNTO ---
            print(f"[OpenSky] Request {suffix.upper()} per {airport_code}...")
            print(f"[AUTH DEBUG] Headers inviati: {headers}")
            print(f"[AUTH DEBUG] URL chiamato: {url}")
            print(f"[AUTH DEBUG] Parametri: {params}")
            response = requests.get(url, params=params, headers=headers, timeout=10)


            print(f"[AUTH DEBUG] STATUS RESPONSE: {response.status_code}")
            print(f"[AUTH DEBUG] RESPONSE BODY print([AUTH DEBUG] RESPONSE BODY (FULL JSON):{response.text}")

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
    start_time = end_time - 24 * 60 * 60

    for (airport_code,) in airports:
        airports_flights(airport_code, start_time, end_time)

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
                            airports_flights(apt, start_t, end_t)
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