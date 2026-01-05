import json
import os
import socket
from confluent_kafka import Consumer, Producer
from prometheus_client import start_http_server, Counter, Gauge

# Configurazione Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'broker_kafka:9092')
TOPIC_IN = 'to-alert-system'
TOPIC_OUT = 'to-notifier'

HOSTNAME = socket.gethostname()

# Metrica COUNTER: Alert processati
ALERTS_PROCESSED = Counter(
    'alerts_processed_total',
    'Total number of airport updates processed from Kafka',
    ['service', 'node']
)

# Metrica GAUGE: Tempo processamento singolo messaggio
PROCESSING_TIME = Gauge(
    'alert_processing_duration_seconds',
    'Time taken to process a single airport update',
    ['service', 'node']
)

# Avvio server metriche su porta 8001
try:
    start_http_server(8001)
    print("[Prometheus] Metrics server active on port 8001", flush=True)
except Exception as e:
    print(f"[Prometheus Error] {e}", flush=True)

consumer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'alert_group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False  # Disabilita il commit automatico
}

producer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'acks': 'all',
    'retries': 3
}
consumer = Consumer(consumer_conf)
producer = Producer(producer_conf)


print(f"[AlertSystem] Avvio servizio. Broker: {KAFKA_BOOTSTRAP_SERVERS}", flush=True)

consumer.subscribe([TOPIC_IN])

def delivery_report(err, msg):
    if err: print(f"Errore invio a Notifier: {err}", flush=True)
    else:
        print(f"[AlertSystem] Messaggio depositato su {msg.topic()}", flush=True)

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None: continue
        if msg.error():
            print(f"Consumer error: {msg.error()}", flush=True)
            continue

        try:
            with PROCESSING_TIME.labels(service='alert_system', node=HOSTNAME).time():
                data = json.loads(msg.value().decode('utf-8'))
                airport = data.get('airport')
                # Qui assumiamo che arrivi count arrivi + partenze, o li sommiamo
                total_arrival = data.get('arrival_count', 0)
                total_departure = data.get('departure_count', 0)
                total_flights = total_arrival + total_departure
                users = data.get('users', []) # Lista di dizionari con email e soglie

                print(f"[AlertSystem] Analisi {airport}: Voli Totali {total_flights}. Utenti da controllare: {len(users)}", flush=True)

                for user in users:
                    email = user['email']
                    high = user.get('high_value')
                    low = user.get('low_value')

                    alert_type = None

                    # Logica di soglia
                    if high is not None and high > 0 and total_flights > high:
                        alert_type = "HIGH_THRESHOLD"
                    elif low is not None and low > 0 and total_flights < low:
                        alert_type = "LOW_THRESHOLD"

                    if alert_type:
                        notification = {
                            "email": email,
                            "subject": f" Alert Voli: {airport} - Soglia {alert_type} Superata",
                            "body": f"""
                            Gentile utente,
                            ti informiamo che l'aeroporto {airport} ha superato la soglia di allerta {alert_type}.
                            
                            RIEPILOGO VOLI:
                            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                            • Totale voli:     {total_flights}
                            • Arrivi (Arr):    {total_arrival}
                            • Partenze (Dep):  {total_departure}
                            """
                        }

                        producer.produce(TOPIC_OUT, json.dumps(notification).encode('utf-8'),callback=delivery_report)
                        producer.poll(0)
                        print(f"[AlertSystem] ALLARME inviato per {email}", flush=True)

                producer.flush()
                consumer.commit(asynchronous=False)

            ALERTS_PROCESSED.labels(service='alert_system', node=HOSTNAME).inc()

        except Exception as e:
            print(f"[AlertSystem] Errore processamento messaggio: {e}", flush=True)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()