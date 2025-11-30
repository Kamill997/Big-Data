version: "3.9"
services:
  user_db:
    image: mysql:8.0
    container_name: container_user_db
    restart: always
    environment:
      MYSQL_ROOT_PASSWORD: root
      MYSQL_DATABASE: user_db
    ports:
      - "3306:3306"
    volumes:
      - ./database/user_db.sql:/docker-entrypoint-initdb.d/user.sql
      #- user_db_data:/var/lib/mysql
    networks:
      - user_net
    healthcheck:
      test: [ "CMD", "mysqladmin", "ping", "-h", "localhost", "-u", "root", "-p$$MYSQL_ROOT_PASSWORD" ]
      interval: 10s
      timeout: 5s
      retries: 5
      #- dsbd_net

  data_db:
    image: mysql:8.0
    container_name: container_data_db
    restart: always
    environment:
      MYSQL_ROOT_PASSWORD: root
      MYSQL_DATABASE: data_db
    ports:
      - "3307:3306"
    volumes:
      - ./database/data_db.sql:/docker-entrypoint-initdb.d/data.sql
      #- data_db_data:/var/lib/mysql
    networks:
      - data_net
    healthcheck:
      test: [ "CMD", "mysqladmin", "ping", "-h", "localhost", "-u", "root", "-p$$MYSQL_ROOT_PASSWORD" ]
      interval: 10s
      timeout: 5s
      retries: 5

  user_manager:
      build: ./user_manager
      container_name: container_user_manager
      ports:
        - "5001:5000"
      environment:
        - DB_HOST=user_db
      depends_on:
        user_db:
          condition: service_healthy
      networks:
        - user_net  # Per parlare col SUO database
        - grpc_net  # Per ricevere chiamate dal Data Collector

  data_collector:
    build: ./data_collector
    container_name: container_data_collector
    ports:
      - "5002:5000"
    environment:
      - DB_HOST=data_db
    depends_on:
      data_db:
        condition: service_healthy
      user_manager:
        condition: service_started
    networks:
      - data_net  # Per parlare col SUO database
      - grpc_net  # Per chiamare lo User Manager
    #client:
    #  build: user_manager
    #container_name: container_user

    #server:
    #build: data_collector
    #container_name: container_data
networks:
  user_net:
    driver: bridge
  data_net:
    driver: bridge
  grpc_net:
    driver: bridge

volumes:
  user_db_data:
  data_db_data:


def sottoscriviInteresse():
    data=request.json
    email=data.get("email")
    if not email:
        return jsonify({"success": False,"message": "Email mancante"}), 400
    esitoVerifica=verify_email_grpc(email)

    if esitoVerifica:
        interessi=data.get("interessi")
        if not interessi:
            return jsonify({"success": False,"message": "Interesse mancante"}), 400
        db=connect_db()
        cursor=db.cursor()
        risultato_operazione = []
        da_scaricare_subito = []

        try:
            for interesse in interessi:
                cursor.execute("SELECT * FROM user_interest WHERE airport_code=%s and email=%s", (interesse,email))
                if cursor.fetchone() is None:
                    insert="""INSERT INTO user_interest (email, airport_code) VALUES (%s, %s)"""
                    cursor.execute(insert, (email, interesse))
                    risultato_operazione.append({"message":f"L'interesse {interesse} è stato registrato correttamente"})
                    da_scaricare_subito.append(interesse)
                else:
                    risultato_operazione.append({"message":f"L'interesse {interesse} era stato già indicato"})
            db.commit()

            if da_scaricare_subito:
                print(f"[TEST] Avvio download immediato per: {da_scaricare_subito}")

                # Impostiamo l'intervallo (es. ultime 2 ore)
                end_t = int(time.time())
                start_t = end_t - 7200

                def quick_fetch():
                    # Questo thread userà la sua connessione DB interna
                    for apt in da_scaricare_subito:
                        # Assicurati che questa funzione sia definita nel tuo file!
                        download_flights_for_airport(apt, start_t, end_t)

                # Avviamo il thread senza bloccare la risposta all'utente
                thread = threading.Thread(target=quick_fetch)
                thread.daemon = True # Si chiude se il programma principale si chiude
                thread.start()
            # =================================================================

            # 3. RESTITUIAMO LA RISPOSTA
            return jsonify({"Esito operazione": risultato_operazione})

        except Exception as e:
            db.rollback()
            return jsonify({"error": f"Qualcosa è andato storto, interessi non registrati"}), 400
        finally:
            cursor.close()
            db.close()
    else:
        return jsonify({"error": f"L'email non è stata mai registrata"}), 422##

        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS flights (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            interested_ICAO VARCHAR(100),
                            ICAO_flight VARCHAR(20) NOT NULL,
                            origin_country VARCHAR(50) NOT NULL,
                            departure_time BIGINT,
                            arrival_time BIGINT,
                            is_arrival BOOLEAN,
                            stored_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                       )
                       """)








def airports_flights(airport_code, start_time, end_time):

    print(f"[DEBUG] Inizio download per aeroporto {airport_code}")

    conn = connect_db()
    cursor = conn.cursor()

    headers = get_opensky_headers()

    # Se l'auth è fallita, rallentiamo drasticamente per non prendere 429
    #if not headers:
     #   print(f"[STOP] IMPOSSIBILE SCARICARE: Credenziali non valide o mancanti.")
      #  print(f"[STOP] Senza token, OpenSky blocca l'IP (429). Salto il download.")
        #time.sleep(5)
        #return

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
            print(f"[AUTH DEBUG] RESPONSE BODY print([AUTH DEBUG] RESPONSE BODY (FULL JSON):{response.text}")

            if response.status_code == 200:
                flights = response.json()
                print(f"[OpenSky] Trovati {len(flights)} voli ({suffix})")

                for flight in flights:
                    # Dati dal JSON
                    icao = flight.get('icao24')
                    #depart_time = flight.get('estDepartureAirport')
                    #arr_time = flight.get('estArrivalAirport')
                    first_seen = flight.get('firstSeen')
                    #last_seen = flight.get('lastSeen')

                    cursor.execute("SELECT icao FROM flights WHERE icao=%s AND departure_time=%s", (icao,first_seen,))

                    if cursor.fetchone():
                        # Il volo esiste già, lo salto (o potrei aggiornarlo)
                        continue

                    # Inserimento nel DB (Flight Data)
                    insert_flights = """
                                     INSERT INTO flights
                                     (icao, departure_airport, arrival_airport, departure_time, arrival_time)
                                     VALUES (%s, %s, %s, %s, %s)
                                     """
                    valori = (
                        flight.get('icao24'),
                        flight.get('estDepartureAirport'),
                        flight.get('estArrivalAirport'),
                        flight.get('firstSeen'),
                        flight.get('lastSeen'),
                    )
                    cursor.execute(insert_flights, valori)
                    count_saved += 1
                    #count_saved += len(flights)
                    # Commit dopo ogni batch (arrival o departure)
                conn.commit()
            elif response.status_code == 401:
                print(f"[OpenSky] ERRORE 401: Token non valido o scaduto! Resetto la cache.")
                global CACHED_TOKEN
                CACHED_TOKEN = None # Forzo il rinnovo al prossimo giro
            elif response.status_code == 404:
                print(f"[OpenSky] Nessun dato trovato per {airport_code} ({suffix})")
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