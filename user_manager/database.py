import mysql.connector
import time
import os

# Configurazione DB (legge le env variables dal docker-compose)
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_USER = "root"
DB_PASSWORD = "root"
DB_NAME = "user_db"

def get_db_connection():
    """Prova a connettersi al database con un ciclo di retry."""
    retries = 5
    while retries > 0:
        try:
            print(f"Tentativo di connessione a {DB_HOST}...")
            conn = mysql.connector.connect(
                host=DB_HOST,
                #port=3306,
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

        print("Inizio creazione tabelle per Data DB...")

        # 3. Tabella user
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS users (
                                                            email VARCHAR(255) PRIMARY KEY,
                           name VARCHAR(100),
                           surname VARCHAR(100)
                           )
                       """)

        print("- Tabella 'user' verificata.")

        # 4. Tabella requestId (Nuova richiesta)
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS requestId (
                                                                id VARCHAR(100) PRIMARY KEY,
                           esito_richiesta VARCHAR(100)
                           )
                       """)

        print("- Tabella 'requestId' verificata.")

        conn.commit()
        cursor.close()
        conn.close()

        print("Inizializzazione Data DB completata con successo.")

    except Exception as e:
        print(f"Errore fatale durante l'init_db: {e}")
        exit(1) # Esce con errore per bloccare il container se il DB fallisce
