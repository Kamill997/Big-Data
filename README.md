# Big Data – Homework 3

Sistema a microservizi per il monitoraggio del traffico aereo basato sui dati di [OpenSky Network](https://opensky-network.org/). Gli utenti si registrano, scelgono gli aeroporti di interesse (codici ICAO) e impostano due soglie sul numero di voli; quando il numero di voli supera la soglia alta o scende sotto la soglia bassa ricevono una **e-mail di alert**.

Questo homework estende i precedenti con due novità:

- **monitoraggio white-box** dei microservizi con **Prometheus** (metriche `Counter` e `Gauge`);
- **migrazione su Kubernetes** (cluster locale creato con **KIND**).

Autori: Gioele Messina, Luca Camiolo. I dettagli di progetto sono nella relazione `Relazione hw3.pdf`.

---

## Architettura

```

![Architettura del sistema](docs/architettura.png)

```

### Microservizi

| Servizio | Cartella | Ruolo | Porta metriche |
|---|---|---|---|
| **User Manager** | `user_manager/` | API REST (Flask) per registrazione/cancellazione utenti, con gestione di richieste idempotenti tramite `id`. Espone un server gRPC per verificare l'esistenza di un utente. | `9102` |
| **Data Collector** | `data_collector/` | API REST (Flask) per gestire gli aeroporti di interesse e interrogare i voli salvati. Ogni **2 ore** scarica da OpenSky arrivi e partenze degli aeroporti di interesse, li salva nel Data DB e pubblica un messaggio su Kafka. Protegge le chiamate esterne con un **Circuit Breaker**. | `8000` |
| **Alert System** | `alert_system/` | Consumer Kafka (`to-alert-system`): confronta il totale voli con le soglie di ogni utente e, se superate, pubblica una notifica su `to-notifier`. | `8001` |
| **Notifier System** | `notifier_system/` | Consumer Kafka (`to-notifier`): invia l'e-mail all'utente via SMTP (Gmail). | `8002` |

### Infrastruttura

- **MySQL 8.0** × 2: `user_db` (tabelle `users`, `requestID`) e `data_db` (tabelle `user_interest`, `flights`).
- **Apache Kafka** (`confluentinc/cp-kafka:7.4.0`, modalità KRaft, singolo broker).
- **NGINX** come API gateway con certificato TLS self-signed (Docker Compose) / **NGINX Ingress Controller** (Kubernetes).
- **Prometheus** `v2.48.0` per la raccolta delle metriche.
- Comunicazione sincrona tra User Manager e Data Collector con **gRPC** (definizione in `proto/user_service.proto`).

---

## Metriche Prometheus

| Metrica | Tipo | Servizio | Descrizione |
|---|---|---|---|
| `user_registered_total` | Counter | User Manager | Registrazioni andate a buon fine |
| `user_deleted_total` | Counter | User Manager | Utenti eliminati |
| `opensky_download_duration_seconds` | Gauge | Data Collector | Durata di download e salvataggio dei voli (label `airport`) |
| `circuit_breaker_failures_total` | Counter | Data Collector | Chiamate fallite o bloccate dal Circuit Breaker (label `error_type`) |
| `alerts_sent_total` | Counter | Alert System | Notifiche inviate al notifier (label `service`, `node`, `email`) |
| `alert_user_processing_duration_seconds` | Gauge | Alert System | Tempo di elaborazione dell'alert per utente (label `service`, `node`, `airport`, `email`) |
| `email_sent_total` | Counter | Notifier System | E-mail inviate agli utenti |

Prometheus è raggiungibile su **http://localhost:9090**.

---

## Struttura della repository

```
Big-Data-Homework-3/
├── user_manager/          # Microservizio gestione utenti (REST + gRPC server)
├── data_collector/        # Microservizio raccolta dati OpenSky (REST + gRPC + scheduler + Circuit Breaker)
├── alert_system/          # Consumer Kafka: valutazione soglie
├── notifier_system/       # Consumer Kafka: invio e-mail
├── proto/                 # Definizione gRPC (user_service.proto)
├── kubernetes/            # Manifest K8s (namespace, deployment, service, ingress, secrets, PVC, ...)
├── kind/                  # Configurazione del cluster KIND (1 control-plane + 1 worker)
├── nginx/                 # Configurazione gateway e certificato self-signed (Docker Compose)
├── prometheus/            # prometheus.yml per Docker Compose
├── Postman/               # Collection per testare le API
├── docker-compose.yml     # Esecuzione alternativa senza Kubernetes
├── commands.bat           # Script di deploy automatico su KIND (Windows)
└── Relazione hw3.pdf      # Relazione del progetto
```

---

## API REST

Tutte le richieste passano dal gateway: `https://localhost:8443/<servizio>/<endpoint>` (certificato self-signed, in Postman disabilitare la verifica SSL).

### User Manager – `/user_manager`

| Metodo | Endpoint | Body (JSON) | Descrizione |
|---|---|---|---|
| `POST` | `/register` | `id`, `email`, `name`, `surname` | Registra un utente (l'`id` rende la richiesta idempotente) |
| `DELETE` | `/delete` | `email` | Elimina l'utente e, via gRPC, i suoi interessi |

### Data Collector – `/data_collector`

| Metodo | Endpoint | Parametri | Descrizione |
|---|---|---|---|
| `POST` | `/subscribeInterest` | `email`, `interessi` (lista ICAO), `high_value`, `low_value` | Aggiunge aeroporti di interesse con le soglie (default: alta `200`, bassa `0`) |
| `DELETE` | `/removeInterest` | `email`, `interessi` | Rimuove aeroporti di interesse |
| `GET` | `/flights/info?icao=...` | `icao` | Informazioni sull'aeroporto |
| `GET` | `/flights/last?airport=...&type=arrival\|departure` | `airport`, `type` | Ultimo arrivo/partenza |
| `GET` | `/flights/average?airport=...&days=...&type=...` | `airport`, `days`, `type` | Media giornaliera dei voli negli ultimi N giorni |
| `POST` | `/modifyTreshold` | `email`, `icao`, `low_value`, `high_value` | Modifica le soglie di alert |

La collection pronta all'uso è in `Postman/` (`UserManager` e `DataCollector`).

---

## Requisiti

- [Docker](https://www.docker.com/) (con Docker Compose)
- [KIND](https://kind.sigs.k8s.io/) e [kubectl](https://kubernetes.io/docs/tasks/tools/) (per il deploy su Kubernetes)
- Credenziali **OpenSky Network** (client id e client secret API)
- Un account **Gmail** con [password per le app](https://support.google.com/accounts/answer/185833) per l'invio delle e-mail
- [Postman](https://www.postman.com/) per testare le API

---

## Configurazione delle credenziali

Le credenziali **non** vanno committate nella repository.

**Kubernetes** – modificare `kubernetes/secrets.yaml` inserendo le credenziali OpenSky e i dati SMTP Gmail.

**Docker Compose** – creare un file `.env` nella root:

```env
# User Manager
USER_DB_HOST=user_db
USER_DB_USER=root
USER_DB_PASSWORD=<password>
USER_DB_NAME=<nome_db>

# Data Collector
DATA_DB_HOST=data_db
DATA_DB_USER=root
DATA_DB_PASSWORD=<password>
DATA_DB_NAME=<nome_db>

# OpenSky API
OPENSKY_CLIENT_ID=<client_id>
OPENSKY_CLIENT_SECRET=<client_secret>

# SMTP Gmail
GMAIL_USER=<indirizzo@gmail.com>
GMAIL_APP_PASSWORD=<app_password>
```

---

## Esecuzione

### Opzione 1 – Kubernetes con KIND (consigliata)

Su Windows basta lanciare lo script:

```bat
commands.bat
```

Lo script esegue in automatico:

1. elimina l'eventuale cluster `bigdata-project` esistente;
2. crea il cluster da `kind/kind_config.yaml` (1 control-plane + 1 worker);
3. installa l'NGINX Ingress Controller;
4. costruisce le immagini Docker dei 4 microservizi e le carica nel cluster;
5. crea il namespace `flights-monitoring`;
6. applica tutti i manifest di `kubernetes/`.

Verificare che i pod siano in esecuzione:

```bash
kubectl get pods -n flights-monitoring
```

Il corretto ordine di avvio è garantito da **initContainers** (`busybox` + `netcat`) che attendono la disponibilità dei servizi da cui ogni pod dipende. I database usano **PersistentVolumeClaim** per non perdere i dati in caso di crash dei pod.

Endpoint esposti dal cluster:

| Risorsa | URL |
|---|---|
| API (Ingress) | `https://localhost:8443/user_manager/...` e `https://localhost:8443/data_collector/...` |
| Prometheus | `http://localhost:9090` |

### Opzione 2 – Docker Compose

```bash
docker compose up --build
```

L'API gateway risponde su `https://localhost:8443`, Prometheus su `http://localhost:53120`.

---

## Esempio di utilizzo

1. Registrare un utente:
   ```http
   POST https://localhost:8443/user_manager/register
   {"id": "<uuid>", "email": "mario.rossi@email.it", "name": "Mario", "surname": "Rossi"}
   ```
2. Iscriversi agli aeroporti di interesse con le soglie:
   ```http
   POST https://localhost:8443/data_collector/subscribeInterest
   {"email": "mario.rossi@email.it", "interessi": ["LIRF", "LSZH"], "high_value": 15, "low_value": 5}
   ```
3. Al successivo ciclo di raccolta (ogni 2 ore) il Data Collector scarica i voli, l'Alert System valuta le soglie e il Notifier invia l'e-mail se necessario.

---

