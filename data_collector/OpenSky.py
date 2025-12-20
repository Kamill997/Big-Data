import requests
import time
import os
import schedule
import json
from database import connect_db
from circuit_breaker import CircuitBreaker,CircuitBreakerOpenException
from confluent_kafka import Producer

OPENSKY_API_URL = "https://opensky-network.org/api/flights"
OPENSKY_TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'broker_kafka:9092')

producer_conf={
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'acks': 'all',
    'retries': 3,
    'linger.ms': 500,
    'batch.size': 16384,
    'max.in.flight.requests.per.connection': 1
}
producer = Producer(producer_conf)

cb = CircuitBreaker(failure_threshold=3, timeout=60, expected_exception=requests.exceptions.RequestException)

class OpenSky:
    def __init__(self):
        self.token = None
        self.client_id = os.getenv('OPENSKY_CLIENT_ID')
        self.client_secret = os.getenv('OPENSKY_CLIENT_SECRET')

    def get_headers(self):
        if not self.token:
            self._refresh_token()
        return {"Authorization": f"Bearer {self.token}"}

    def _refresh_token(self):
        if not self.client_id or not self.client_secret:
            print("[OpenSky] Credenziali mancanti (controlla il .env).", flush=True)
            return

        print("[OpenSky] Rigenerazione Token...", flush=True)
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
        try:
            response = requests.post(OPENSKY_TOKEN_URL, data, timeout=10)
            if response.status_code == 200:
                self.token = response.json().get("access_token")
                print(f"[OpenSky] Nuovo Token generato.", flush=True)
            else:
                print(f"[OpenSky] Errore Token: {response.status_code} - {response.text}", flush=True)
        except Exception as e:
            print(f"[OpenSky] Errore auth: {e}", flush=True)

    def _make_request(self, url, params, headers=None):
        return requests.get(url, params=params, headers=headers, timeout=10)

    def api_credits(self, response):
        try:
            # OpenSky usa headers standard tipo X-Rate-Limit-Remaining
            remaining = response.headers.get("X-Rate-Limit-Remaining", "N/A")
            limit = response.headers.get("X-Rate-Limit-Limit", "N/A")

            # Se non trova quelli standard, a volte usa nomenclature diverse o non li manda su errore
            if remaining != "N/A":
                print(f"[OpenSky Quota] Crediti Rimanenti: {remaining} / Limite Giornaliero: {limit}", flush=True)
            else:
                # Debug: se non trovi gli header, stampa le chiavi disponibili per capire come si chiamano
                # print(f"[DEBUG HEADERS] {response.headers.keys()}")
                pass
        except Exception as e:
            print(f"[Quota Error] Impossibile leggere quota: {e}")

    def airports_flights(self, airport_code, start_time, end_time):
        print(f"[DEBUG] Download voli per {airport_code}", flush=True)

        header = self.get_headers()
        if not header or not header.get("Authorization"):
            print("[OpenSky] Skip download: No Token.", flush=True)
            return

        conn = connect_db()
        cursor = conn.cursor()

        total_arrival = 0
        total_departure = 0
        endpoints = [("arrival", True), ("departure", False)]
        count_saved = 0

        for suffix, is_arrival in endpoints:
            url = f"{OPENSKY_API_URL}/{suffix}"
            params = {'airport': airport_code, 'begin': start_time, 'end': end_time}

            try:
                #Utilizzo del Circuit breaker
                print(f"[OpenSky] Request {suffix.upper()} per {airport_code}...")
                response = cb.call(self._make_request, url, params,header)

                self.api_credits(response)

                if response.status_code == 401:
                    print("[OpenSky] Token scaduto, rigenero...")
                    #self.token = None
                    self._refresh_token()
                    header = self.get_headers()
                    response = requests.get(url, params=params, headers=header, timeout=10)
                    self.api_credits(response)

                if response.status_code == 200:
                    flights = response.json()
                    num_originali = len(flights)
                    if is_arrival:
                        total_arrival = num_originali
                    else:
                        total_departure = num_originali

                    print(f"[OpenSky] Trovati {len(flights)} voli ({suffix})")

                    flights = flights[:50]
                    print(f"[OpenSky] Trovati {num_originali} voli. Ne processo {len(flights)}.")

                    batch_voli = []
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
                        insert_flights = """
                                         INSERT INTO flights (icao, departure_airport, arrival_airport, departure_time, arrival_time)
                                         VALUES (%s, %s, %s, %s, %s)
                                         ON DUPLICATE KEY UPDATE
                                            departure_airport = IF(VALUES(departure_airport) IS NOT NULL, VALUES(departure_airport), flights.departure_airport),
                                            arrival_airport   = IF(VALUES(arrival_airport)   IS NOT NULL, VALUES(arrival_airport),   flights.arrival_airport),
                                            departure_time = IF(VALUES(departure_time) IS NOT NULL, VALUES(departure_time), flights.departure_time),
                                            arrival_time   = IF(VALUES(arrival_time)   IS NOT NULL, VALUES(arrival_time),   flights.arrival_time) 
                                         """

                        print(f"[DB] Scrivo {len(batch_voli)} voli nel database...")
                        cursor.executemany(insert_flights, batch_voli)
                        conn.commit()
                        count_saved += len(batch_voli)
                        print(f"[DB] Scrittura completata per {suffix}.")

                elif response.status_code == 404:
                    print(f"[OpenSky] Nessun dato trovato per {airport_code} ({suffix})")
                    pass
                elif response.status_code == 429:
                    print(f"[OpenSky] ERRORE 429: Troppe richieste!")
                    time.sleep(5)
                else:
                    print(f"[OpenSky] Errore {response.status_code}: {response.text}")

            except CircuitBreakerOpenException:
                print(f"[CircuitBreaker] Aperto per {airport_code}. Chiamata bloccata.")
                break

            except Exception as e:
                print(f"[OpenSky] Eccezione: {e}")

            time.sleep(2)

        cursor.close()
        conn.close()
        print(f"[OpenSky] {airport_code}: Salvati {count_saved} voli totali (Arr+Dep).")
        self.notify_alert_system(airport_code, total_arrival, total_departure)

    def notify_alert_system(self, airport_code, arr, dep):
        try:
            conn = connect_db()
            cursor = conn.cursor(dictionary=True)
            # Recupero gli utenti interessati e le loro soglie
            cursor.execute("SELECT email, high_value, low_value FROM user_interest WHERE airport_code = %s", (airport_code,))
            users = cursor.fetchall()
            conn.close()

            if not users: return

            payload = {
                "airport": airport_code,
                "arrival_count": arr,
                "departure_count": dep,
                "users": users # Passo la lista utenti al microservizio successivo
            }

            # Invio messaggio al topic 'to-alert-system'
            producer.produce('to-alert-system', json.dumps(payload).encode('utf-8'))
            producer.flush()
            print(f"[Kafka] Inviato update per {airport_code} (Arr:{arr}/Dep:{dep})", flush=True)
        except Exception as e:
            print(f"[Kafka Error] Impossibile inviare messaggio: {e}")

    def fetch_opensky_data(self):
        print("[Scheduler] Inizio ciclo download...")
        try:
            conn = connect_db()
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT airport_code FROM user_interest")
            airports = cursor.fetchall()
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"Errore lettura DB: {e}")
            return

        end_time = int(time.time())
        start_time = end_time - 3600

        if not airports:
            print("Nessun interesse attivo.")
            return

        for (airport_code,) in airports:
            self.airports_flights(airport_code, start_time, end_time)
            time.sleep(3)


opensky_service = OpenSky()

def run_scheduler():
    schedule.every(2).hours.do(opensky_service.fetch_opensky_data)
    print("[Scheduler] Scheduler avviato.")
    while True:
        schedule.run_pending()
        time.sleep(1)