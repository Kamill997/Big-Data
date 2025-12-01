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

CACHED_TOKEN = None
TOKEN_EXPIRY = 0

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
                print(f"Client ID caricato: {CLIENT_ID}")
            else:
                print("File credenziali presente ma clientId o clientSecret mancanti.")
    else:
        print("File credenziali non trovato.")
except Exception as e:
    print(f"Errore lettura credenziali: {e}")

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
    global CACHED_TOKEN, TOKEN_EXPIRY

    if CACHED_TOKEN and time.time() < (TOKEN_EXPIRY - 60):
        return {"Authorization": f"Bearer {CACHED_TOKEN}"}

    if not CLIENT_ID or not CLIENT_SECRET:
        print("[AUTH] Credenziali mancanti, procedo in modalità anonima.")
        return None

    print(f"[AUTH DEBUG] Sto usando CLIENT_ID='{CLIENT_ID}'")
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }

    try:
        print("[AUTH DEBUG] Invio richiesta token a OpenSky...")
        # Chiamata per ottenere il token
        response = requests.post(OPENSKY_TOKEN_URL, data=data, timeout=10)

        print(f"[AUTH DEBUG] Status Token Response: {response.status_code}")
        print(f"[AUTH DEBUG] Body Token Response: {response.text[:200]}...")

        if response.status_code == 200:
            token = response.json()
            CACHED_TOKEN = token.get("access_token")

            expires_in = token.get("expires_in", 3600)
            TOKEN_EXPIRY = time.time() + expires_in
            print(f"[AUTH] Nuovo token ottenuto. Scade tra {expires_in}s.")
            return {"Authorization": f"Bearer {CACHED_TOKEN}"}
        else:
            print(f"[AUTH] Errore richiesta token: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"[AUTH] Eccezione richiesta token: {e}")
        return None

def airports_flights(airport_code, start_time, end_time):

    print(f"[DEBUG] Inizio download per aeroporto {airport_code}")

    conn = connect_db()
    cursor = conn.cursor()

    headers = get_opensky_headers()


    # Tipi di volo da scaricare
    endpoints = [
        ("arrival", True),
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
            print(f"[OpenSky] Request {suffix.upper()} per {airport_code}...")
            response = requests.get(url, params=params, headers=headers, timeout=10)
            print(f"[AUTH DEBUG] RESPONSE BODY (200 chars): {response.text[:200]}")


            if response.status_code == 200:
                flights = response.json()
                num_originali = len(flights)
                print(f"[OpenSky] Trovati {len(flights)} voli ({suffix})")

                flights=flights[:50]
                print(f"[OpenSky] Trovati {num_originali} voli. Ne processo {len(flights)}.")

                if not flights: continue

                batch_voli=[]
                for flight in flights:
                    valori = (
                        flight.get('icao24'),
                        flight.get('estDepartureAirport'),
                        flight.get('estArrivalAirport'),
                        flight.get('firstSeen'),
                        flight.get('lastSeen'),
                    )
                    batch_voli.append(valori)

                if batch_voli:
                    # Inserimento nel DB (Flight Data)
                    insert_flights = """
                                     INSERT INTO flights
                                     (icao, departure_airport, arrival_airport, departure_time, arrival_time)
                                     VALUES (%s, %s, %s, %s, %s)
                                     ON DUPLICATE KEY UPDATE
                                                departure_airport = IF(VALUES(departure_airport) IS NOT NULL, VALUES(departure_airport), flights.departure_airport),
                                                arrival_airport   = IF(VALUES(arrival_airport)   IS NOT NULL, VALUES(arrival_airport),   flights.arrival_airport),
                                                departure_time = IF(VALUES(departure_time) IS NOT NULL, VALUES(departure_time), flights.departure_time),
                                                arrival_time   = IF(VALUES(arrival_time)   IS NOT NULL, VALUES(arrival_time),   flights.arrival_time)
                                     """
                    #departure_airport=COALESCE(flights.departure_airport,VALUES(departure_airport)),
                    #arrival_airport=COALESCE(flights.arrival_airport,values(arrival_airport)),
                    #arrival_time=VALUES(arrival_time)

                    print(f"[DB] Scrivo {len(batch_voli)} voli nel database...",flush=True)
                    cursor.executemany(insert_flights, batch_voli)
                    conn.commit()
                    #count_saved += 1
                    count_saved += len(batch_voli)
                    print(f"[DB] Scrittura completata per {suffix}.",flush=True)

            elif response.status_code == 401:
                print(f"[OpenSky] ERRORE 401: Token non valido o scaduto! Resetto la cache.")
                global CACHED_TOKEN
                CACHED_TOKEN = None # Forzo il rinnovo al prossimo giro
            elif response.status_code == 404:
                print(f"[OpenSky] Nessun dato trovato per {airport_code} ({suffix})")
                pass
            elif response.status_code == 429:
                print(f"[OpenSky] ERRORE 429: Troppe richieste! Rallentare.")
                time.sleep(10) # Pausa di emergenza lunga
            else:
                print(f"[OpenSky] Errore {response.status_code}: {response.text}")

        except Exception as e:
            print(f"[OpenSky] Errore connessione: {e}")

        time.sleep(2)

    cursor.close()
    conn.close()
    print(f"[OpenSky] {airport_code}: Salvati {count_saved} voli totali (Arr+Dep).")

#SCHEDULER
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
    start_time = end_time - 8*3600 #24 * 60 * 60

    if not airports:
        print("[Ciclo] Nessun interesse attivo.")
        return

    for (airport_code,) in airports:
        airports_flights(airport_code, start_time, end_time)
        print("[Ciclo] Pausa di 3s...")
        time.sleep(3)

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

    if verify_email_grpc(email):
        interessi = data.get("interessi")
        if not interessi:
            return jsonify({"success": False, "message": "Interesse mancante"}), 400

        if isinstance(interessi, str):
            interessi = [interessi]

        db = connect_db()
        cursor = db.cursor()
        ris = []
        daScaricare = []
        # Lista per tenere traccia di cosa scaricare subito

        try:
            for interesse in interessi:

                # QUERY DI CONTROLLO
                #cursor.execute("SELECT * FROM user_interest WHERE airport_code=%s and email=%s", (interesse, email))
                cursor.execute("INSERT IGNORE INTO user_interest (email, airport_code) VALUES (%s, %s)", (email, interesse))

                if cursor.rowcount > 0:
                    msg = f"Interesse {interesse} aggiunto."

                # 2. LOGICA INTELLIGENTE: Devo scaricare i dati?
                # Conto quanti utenti monitorano questo aeroporto ORA.
                    cursor.execute("SELECT COUNT(*) FROM user_interest WHERE airport_code = %s", (interesse,))
                    count = cursor.fetchone()[0]

                # Se count è 1, significa che prima era 0. È NUOVO per il sistema!
                    if count == 1:
                        daScaricare.append(interesse)
                        msg += " (Nuovo aeroporto -> Avvio download storico)"
                    else:
                        msg += " (Dati già presenti nel sistema)"
                    ris.append(msg)
                else:
                    ris.append(f"Interesse {interesse} già presente per te.")

                #if cursor.fetchone() is None:
                    # È NUOVO -> LO INSERISCO
                #    cursor.execute("INSERT INTO user_interest (email, airport_code) VALUES (%s, %s)",(email, interesse))
                #    ris.append({"message": f"L'interesse {interesse} è stato registrato correttamente"})
                    # AGGIUNGO ALLA LISTA DOWNLOAD
                #    daScaricare.append(interesse)
                #    print(f"[DEBUG] {interesse} è nuovo -> Aggiunto alla coda di download.")
                #else:
                    # ESISTEVA GIÀ -> NON LO SCARICO
                 #   ris.append({"message": f"L'interesse {interesse} era stato già indicato"})
                  #  print(f"[DEBUG] {interesse} esisteva già -> Niente download.")

            db.commit()
            print(f"Da scaricare: {daScaricare}")
            # =================================================================
            # AVVIO DOWNLOAD IMMEDIATO
            # =================================================================
            if daScaricare:
                print(f"[TEST] Avvio thread per: {daScaricare}")
                def quick_fetch():
                    print("[THREAD] quick_fetch avviato con:", daScaricare)
                    end_time = int(time.time())
                    start_time = end_time - 8*3600   #24 * 60 * 60)
                    try:
                        print("[THREAD] Thread partito...")
                        for inte in daScaricare:
                            print(f"[THREAD] Chiamo download_flights per {inte}")
                            airports_flights(inte, start_time, end_time)
                            time.sleep(2)
                        print("[THREAD] Finito.")
                    except Exception as thread_e:
                        print(f"[THREAD ERROR] Errore nel thread: {thread_e}")

                thread = threading.Thread(target=quick_fetch, daemon=True)
                #thread.daemon = True
                thread.start()
            else:
                print("[DEBUG] Nessun nuovo interesse da scaricare.")
            return jsonify({"Esito operazione": ris})

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

    if verify_email_grpc(email):
        interessi=data.get("interessi")
        if not interessi:
            return jsonify({"Error":"Non hai indicato un interesse"}), 400

        db=connect_db()
        cursor=db.cursor()
        ris = []
        try:
            for interesse in interessi:
                cursor.execute("SELECT * FROM user_interest WHERE email=%s and airport_code=%s", (email,interesse))

                if cursor.fetchone():
                    cursor.execute("DELETE FROM user_interest WHERE email=%s AND airport_code=%s", (email,interesse))
                    ris.append({"Message":f"L'interesse {interesse} è stato eliminato"})
                else:
                    ris.append({"Message": f"L'interesse {interesse} non è presente"})

            db.commit()
            return jsonify({"Esito operazione": ris})

        except Exception as e:
            db.rollback()
            return jsonify({"Error":f"Qualcosa è andato storto e gli interessi non sono stati eliminati"})
        finally:
            cursor.close()
            db.close()
    else:
        return jsonify({"Error":"Email non registrata"}),422

@app.get('/flights/last')
def last_flights():
    airport = request.args.get('airport')
    flight_type = request.args.get('type', 'departure') # 'departure' o 'arrival'

    if not airport:
        return jsonify({"error": "Airport code mancante"}), 400

    conn = connect_db()
    # dictionary=True è utile se usi mysql-connector puro per avere JSON facile,
    # ma col tuo setup standard cursore a tuple va bene, formattiamo a mano:
    cursor = conn.cursor()

    try:
        if flight_type == 'departure':
            # Ultimo volo PARTITO da qui
            query = """
                    SELECT icao, departure_airport, arrival_airport, departure_time, arrival_time
                    FROM flights
                    WHERE departure_airport = %s
                    ORDER BY departure_time DESC
                    LIMIT 1 
                    """
        else:
            # Ultimo volo ARRIVATO qui
            query = """
                    SELECT icao, departure_airport, arrival_airport, departure_time, arrival_time
                    FROM flights
                    WHERE arrival_airport = %s
                    ORDER BY arrival_time DESC
                    LIMIT 1 
                    """

        cursor.execute(query, (airport,))
        row = cursor.fetchone()

        if row:
            # Convertiamo la tupla in dizionario per il JSON
            flight_data = {
                "icao": row[0],
                "departure": row[1],
                "arrival": row[2],
                "dep_time": row[3],
                "arr_time": row[4]
            }
            return jsonify({"airport": airport, "type": flight_type, "last_flight": flight_data})
        else:
            return jsonify({"message": f"Nessun volo registrato per {airport}"}), 404

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@app.get('/flights/average')
def average_flights():
    # Esempio: GET /stats/average?airport=LIRF&days=7&type=departure
    airport = request.get('airport')
    days = request.get('days')
    flight_type = request.get('type', 'departure')

    if not airport:
        return jsonify({"error": "Airport code mancante"}), 400

    conn = connect_db()
    cursor = conn.cursor()

    try:
        # Calcolo il timestamp di X giorni fa
        # time.time() è in secondi. Giorni * 24h * 60m * 60s
        cutoff_time = int(time.time()) - (days * 86400)

        if flight_type == 'departure':
            query = "SELECT COUNT(*) FROM flights WHERE departure_airport = %s AND departure_time > %s"
        else:
            query = "SELECT COUNT(*) FROM flights WHERE arrival_airport = %s AND arrival_time > %s"

        cursor.execute(query, (airport, cutoff_time))
        total_flights = cursor.fetchone()[0]

        # Calcolo media
        average = total_flights / days if days > 0 else 0

        return jsonify({
            "airport": airport,
            "days_analyzed": days,
            "type": flight_type,
            "total_flights": total_flights,
            "average_per_day": round(average, 2)
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@app.get('/debug/duplicati')
def debug_duplicati():
    """Endpoint per controllare voli duplicati"""
    try:
        db = connect_db()
        cursor = db.cursor(dictionary=True)

        # Cerca possibili duplicati
        cursor.execute("""
                       SELECT icao, departure_time, arrival_time, COUNT(*) as count
                       FROM flights
                       GROUP BY icao, departure_time, arrival_time
                       HAVING COUNT(*) > 1
                       ORDER BY count DESC
                       """)

        duplicati = cursor.fetchall()

        cursor.close()
        db.close()

        return jsonify({
            "duplicati_trovati": len(duplicati),
            "duplicati": duplicati
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    time.sleep(5)
    init_db()
    scheduler_thread = threading.Thread(target=run_scheduler)
    scheduler_thread.daemon = True # Si chiude quando chiudi l'app
    scheduler_thread.start()
    app.run(host="0.0.0.0", port=5000, debug=True)