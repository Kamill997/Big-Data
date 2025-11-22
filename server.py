import json
import pandas as pd, requests
from math import radians, sin, cos, sqrt, atan2

# ===============================
# FUNZIONE DISTANZA (HAVERSINE)
# ===============================
def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # raggio Terra in km
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    return R * c


# ===============================
# CARICA DATABASE AEROPORTI
# ===============================
airports = pd.read_csv("airports.csv", low_memory=False)
airports = airports[airports["type"].isin(["medium_airport", "large_airport"])]
airports = airports[["name", "iata_code", "icao_code", "latitude_deg", "longitude_deg"]]


# ===============================
# FUNZIONE PER TROVARE L'AEROPORTO PIÙ VICINO
# ===============================
def nearest_airport(lat, lon, max_distance_km=15):
    airports["distance"] = airports.apply(
        lambda row: haversine(lat, lon, row["latitude_deg"], row["longitude_deg"]),
        axis=1
    )
    closest = airports.sort_values("distance").iloc[0]

    if closest["distance"] <= max_distance_km:
        return {
            "name": closest["name"],
            "iata": closest["iata_code"],
            "icao": closest["icao_code"],
            "distance_km": float(closest["distance"])
        }
    else:
        return None


# =========================================
# 1️⃣ CHIAMATA API OPENSKY
# =========================================
print("📡 Richiesta dati da OpenSky...")
url = "https://opensky-network.org/api/flights/arrival"
response = requests.get(url)

if response.status_code != 200:
    print("❌ Errore nella richiesta API:", response.status_code)
    exit()

data = response.json()
states = data.get("states", [])

print(f"🔍 Recuperati {len(states)} voli totali.")

# =========================================
# 2️⃣ FILTRAGGIO VOLI A TERRA (GROUND)
# =========================================
grounded = []

for s in states:
    if len(s) < 9:
        continue  # entry malformata

    icao24 = s[0]
    callsign = s[1].strip() if s[1] else None
    lon = s[5]
    lat = s[6]
    on_ground = s[8]

    # condizioni essenziali
    if on_ground and lat is not None and lon is not None:
        airport = nearest_airport(lat, lon)
        grounded.append({
            "icao24": icao24,
            "callsign": callsign,
            "latitude": lat,
            "longitude": lon,
            "airport": airport  # può essere None, sempre presente
        })


# =========================================
# 3️⃣ RISULTATI FINALI
# =========================================
print("\n==============================")
print("✈️  VOLI GROUND + AEROPORTO")
print("==============================")

if not grounded:
    print("Nessun volo grounded trovato.")
else:
    for f in grounded:
        print("\n---------------------------------")
        print("Volo:", f["callsign"], "-", f["icao24"])
        print("Posizione:", f["latitude"], f["longitude"])

        if f["airport"] is not None:
            ap = f["airport"]
            print("Aeroporto più vicino:")
            print("  Nome:", ap["name"])
            print("  ICAO:", ap["icao"])
            print("  IATA:", ap["iata"])
            print("  Distanza:", round(ap["distance_km"], 2), "km")
        else:
            print("❗ Nessun aeroporto entro 15 km.")
