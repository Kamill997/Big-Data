import requests
import json

with open("../credentials.json", "r") as file:
    credentials = json.load(file)
    CLIENT_ID = credentials["clientId"]
    CLIENT_SECRET = credentials["clientSecret"]
url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

# Dati da inviare come form
data = {
    "grant_type": "client_credentials",
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET
}

# POST request
response = requests.post(url, data)

# Controlla che la richiesta sia andata a buon fine
if response.status_code == 200:
    token = response.json().get("access_token")
    print("Il token è:", token)
else:
    print("Errore:", response.status_code, response.text)

richiesta_dati = {
    "airport": "KLAS",            # aeroporto di Francoforte
    "begin": 1763567460,          # timestamp UNIX inizio
    "end": 1763653860             # timestamp UNIX fine
    }

# Header con Authorization Bearer
headers = {
    "Authorization": f"Bearer {token}"
}

# Richiesta GET
response = requests.get("https://opensky-network.org/api/flights/arrival", headers=headers, params=richiesta_dati)
# Controllo della risposta
if response.status_code == 200:
    data = response.json()
    # Stampa JSON in modo leggibile
    print(json.dumps(data, indent=4))
else:
    print("Errore:", response.status_code, response.text)
