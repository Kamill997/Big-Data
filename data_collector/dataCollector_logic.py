from database import connect_db
from gRPC_Logic import verify_email_grpc
from OpenSky import opensky_service
import asyncio
import threading
import time
from circuit_breaker import CircuitBreaker, CircuitBreakerOpenException

circuit_breaker = CircuitBreaker(failure_threshold=5, timeout=5)

class DataCollectorLogic:
    @staticmethod
    async def subscribeInterest(email, interessi):

        try:
            exists = await (verify_email_grpc(email))
        except Exception as e:
            return False, f"Errore verifica utente: {str(e)}", 500

        if not exists:
            return False, "Email non registrata", 400

        if not interessi:
            return False, "Interessi mancanti", 400

        if isinstance(interessi, str):
            interessi = [interessi]

        db = connect_db()
        cursor = db.cursor()
        ris = []
        da_scaricare = []

        try:
            for interesse in interessi:
                # Controllo se esiste già l'interesse per questo utente
                cursor.execute("SELECT * FROM user_interest WHERE airport_code=%s AND email=%s", (interesse, email))
                if cursor.fetchone() is None:
                    # Inserisce il nuovo interesse
                    cursor.execute("INSERT INTO user_interest (email, airport_code) VALUES (%s, %s)", (email, interesse))
                    msg = f"L'interesse {interesse} è stato registrato correttamente"

                    # Controllo se è un aeroporto nuovo e mai stato monitorato prima
                    cursor.execute("SELECT COUNT(*) FROM user_interest WHERE airport_code = %s", (interesse,))
                    count = cursor.fetchone()[0]

                    if count == 1:
                        da_scaricare.append(interesse)
                        msg += " (Nuovo aeroporto -> Avvio download)"
                    else:
                        msg += " (Dati già presenti)"
                    ris.append(msg)
                else:
                    ris.append({"message": f"L'interesse {interesse} era già presente"})

            db.commit()

            #Utilizzo thread nel caso debba scaricare voli per via di una nuova sottoscrizione
            if da_scaricare:
                def quick_fetch():
                    print(f"[THREAD] Download per: {da_scaricare}")
                    end_time = int(time.time())
                    start_time = end_time - 8*3600

                    try:
                        for inte in da_scaricare:
                            print(f"Chiamo airports_flights per {inte}")
                            result=circuit_breaker.call(opensky_service.airports_flights,inte, start_time, end_time)
                            time.sleep(2)
                            print("Il Download è terminato.")
                    except CircuitBreakerOpenException:
                        print(f"Circuit è aperto. La chiamata al servizio di OpenSky non verrà effettuata.")
                    except Exception as e:
                        print(f"Eccezione: - {e}")

                threading.Thread(target=quick_fetch, daemon=True).start()
            else:
                print("Nessun nuovo interesse da scaricare.")
            return True, ris, 200

        except Exception as e:
            db.rollback()
            return False, f"Errore DB: {str(e)}", 500
        finally:
            cursor.close()
            db.close()

    @staticmethod
    async def removeInterest(email, interessi):
        try:
            exists = await (verify_email_grpc(email))
        except Exception as e:
            return False, f"Errore verifica utente: {str(e)}", 500

        if not exists:
            return False, "Email non registrata", 400

        if not interessi:
            return False, "Interessi mancanti", 400

        if isinstance(interessi, str): interessi = [interessi]

        db = connect_db()
        cursor = db.cursor()
        ris = []
        try:
            for interesse in interessi:
                cursor.execute("SELECT * FROM user_interest WHERE email=%s AND airport_code=%s", (email, interesse))
                if cursor.fetchone():
                    cursor.execute("DELETE FROM user_interest WHERE email=%s AND airport_code=%s", (email, interesse))
                    ris.append({"Message": f"L'interesse {interesse} è stato eliminato"})
                else:
                    ris.append({"Message": f"L'interesse {interesse} non è presente"})
            db.commit()
            return True, ris, 200
        except Exception as e:
            db.rollback()
            return False, str(e), 500
        finally:
            cursor.close()
            db.close()

    @staticmethod
    async def showInfo(email, icao):
        try:
            exists = await (verify_email_grpc(email))
        except Exception as e:
            return False, f"Errore verifica utente: {str(e)}", 500

        if not exists:
            return False, "Email non registrata", 400

        db = connect_db()
        cursor = db.cursor(dictionary=True)
        try:
            cursor.execute("SELECT 1 FROM user_interest WHERE email=%s AND airport_code=%s", (email, icao))
            if not cursor.fetchone():
                return False, "Non hai registrato questo interesse", 400

            cursor.execute("SELECT * FROM flights WHERE arrival_airport=%s OR departure_airport=%s", (icao, icao))
            results = cursor.fetchall()

            if not results:
                return False, "Nessuna info trovata per questo ICAO", 404

            return True, {"flights": results}, 200
        finally:
            cursor.close()
            db.close()

    @staticmethod
    async def last_flights(email, airport, flight_type):
        try:
            exists = await (verify_email_grpc(email))
        except Exception as e:
            return False, f"Errore verifica utente: {str(e)}", 500

        if not exists:
            return False, "Email non registrata", 400

        db = connect_db()
        cursor = db.cursor(dictionary=True)
        try:
            if flight_type == 'departure':
                cursor.execute("SELECT icao, departure_airport, arrival_airport, departure_time, arrival_time FROM flights WHERE departure_airport = %s ORDER BY arrival_time DESC LIMIT 1",
                           (airport,))
            else:
                cursor.execute("SELECT icao, departure_airport, arrival_airport, departure_time, arrival_time FROM flights WHERE arrival_airport = %s ORDER BY arrival_time DESC LIMIT 1",
                           (airport,))

            row = cursor.fetchone()

            if row:
                return True, {"esito": row, "tipo": flight_type}, 200
            else:
                return False, f"Nessun volo registrato per {airport}", 404
        finally:
            cursor.close()
            db.close()

    @staticmethod
    async def average_flights(email, airport, days, flight_type):
        try:
            exists = await (verify_email_grpc(email))
        except Exception as e:
            return False, f"Errore verifica utente: {str(e)}", 500

        if not exists:
            return False, "Email non registrata", 400

        db = connect_db()
        cursor = db.cursor()
        try:
            period = int(time.time()) - (days * 86400)

            if flight_type == 'departure':
                cursor.execute("SELECT COUNT(*) FROM flights WHERE departure_airport = %s AND departure_time > %s" , (airport, period))
            else:
                cursor.execute("SELECT COUNT(*) FROM flights WHERE arrival_airport = %s AND arrival_time > %s" , (airport, period))

            total_flights = cursor.fetchone()[0]
            average = total_flights / days if days > 0 else 0

            data = {
                "airport": airport,
                "days_analyzed": days,
                "type": flight_type,
                "total_flights": total_flights,
                "average_per_day": round(average, 2)
            }
            return True, data, 200
        finally:
            cursor.close()
            db.close()