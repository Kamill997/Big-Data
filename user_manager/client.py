import requests
import json

BASE_URL = "http://127.0.0.1:5001"

def registra_utente(email, nome, cognome):

    endpoint = f"{BASE_URL}/register"

    # I dati da inviare (il payload JSON)
    dati_utente = {
        "email": email,
        "name": nome,
        "surname": cognome
    }

    print(f"--- Tentativo di registrazione per: {email} ---")

    try:
        # requests.post fa la chiamata HTTP
        response = requests.post(endpoint, json=dati_utente)

        # Stampiamo il codice di stato
        print(f"Status Code: {response.status_code}")

        # Stampiamo la risposta del server (il JSON)
        try:
            print(f"Risposta Server: {response.json()}")
        except:
            print(f"Risposta Server (Testo): {response.text}")

    except requests.exceptions.ConnectionError:
        print("ERRORE: Impossibile connettersi al server. Hai avviato app.py?")

def elimina_utente(email):
    endpoint = f"{BASE_URL}/delete"

    dati_utente = {
        "email": email
    }

    try:
        response = requests.delete(endpoint, json=dati_utente)
        print(f"Status Code: {response.status_code}")
        # Stampiamo la risposta del server (il JSON)
        try:
            print(f"Risposta Server: {response.json()}")
        except:
            print(f"Risposta Server (Testo): {response.text}")

    except requests.exceptions.ConnectionError:
        print("ERRORE: Impossibile connettersi al server. Hai avviato app.py?")


if __name__ == "__main__":
    # Registro prima volta


    print("\n" + "="*30 + "\n")

    # Registro stesso utente
    registra_utente("mgugario.rossi@email.it", "Mario", "Rossi")

    print("\n" + "="*30 + "\n")

    elimina_utente("hello@gmail.com")
