import json
import os
import sys
from confluent_kafka import Consumer, Producer

# Configurazione Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'broker_kafka:9092')
TOPIC_IN = 'to-alert-system'
TOPIC_OUT = 'to-notifier'

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

            #producer.poll(0)
            producer.flush()
            consumer.commit(asynchronous=False)

        except Exception as e:
            print(f"[AlertSystem] Errore processamento messaggio: {e}", flush=True)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()