import mysql.connector
import time
import os

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_USER = "root"
DB_PASSWORD = "root"
DB_NAME = "user_db"

def get_db_connection():
    retries = 5
    while retries > 0:
        try:
            print(f"Tentativo di connessione a {DB_HOST}...")
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
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
        cursor.execute(f"USE {DB_NAME}")

        print("Inizio creazione tabelle per User DB.")

        # 1. Tabella user
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                email VARCHAR(255) PRIMARY KEY,
                name VARCHAR(100),
                surname VARCHAR(100)
            )
        """)

        # 2. Tabella requestId
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS requestID (
                id VARCHAR(100) PRIMARY KEY,
                esito_richiesta VARCHAR(100)
            )
        """)

        conn.commit()
        cursor.close()
        conn.close()

        print("Inizializzazione User DB completata con successo.")

    except Exception as e:
        print(f"Errore fatale durante l'init_db: {e}")
        exit(1)
