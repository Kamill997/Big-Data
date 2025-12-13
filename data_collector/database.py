import mysql.connector
import time
import os


DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_USER = os.getenv('DB_USER','root')
DB_PASSWORD = os.getenv('DB_PASSWORD','root')
DB_NAME = os.getenv('DB_NAME','data_db')


def connect_db():
    retries = 5
    while retries > 0:
        try:
            print(f"Tentativo di connessione a {DB_HOST}/{DB_NAME}...")
            conn = mysql.connector.connect(
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME
            )
            print("Connessione al DB riuscita!")
            return conn
        except mysql.connector.Error as err:
            print(f"Database non pronto ({err}). Riprovo tra 5 secondi...")
            retries -= 1
            time.sleep(5)
    raise Exception("Impossibile connettersi al Database.")

def init_db():
    try:
        conn = connect_db()
        cursor = conn.cursor()

        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
        cursor.execute(f"USE {DB_NAME}")

        # 1. Tabella user_interest
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS user_interest (
                                id INT AUTO_INCREMENT PRIMARY KEY,
                                email VARCHAR(255),
                                airport_code VARCHAR(10),
                                high_value INT NOT NULL,
                                low_value INT NOT NULL,
                                UNIQUE(email, airport_code)
                       )
                       """)

        # 2. Tabella flights
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS flights(
                           id INT AUTO_INCREMENT PRIMARY KEY,
                           icao VARCHAR(50),
                           departure_airport VARCHAR(20),
                           arrival_airport   VARCHAR(20),
                           departure_time BIGINT,
                           arrival_time  BIGINT,
                           UNIQUE KEY unique_flight (icao, departure_time)
                       )
                    """)

        conn.commit()
        cursor.close()
        conn.close()

        print("Inizializzazione Data DB completata.")

    except Exception as e:
        print(f"Errore durante l'init_db: {e}")
        exit(1)
