import json
import os
from confluent_kafka import Consumer

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka_broker:9092')

consumer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'notifier_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_conf)
TOPIC_IN = 'to-notifier'

print(f"[NotifierSystem] Avvio servizio mailer simulato.", flush=True)
consumer.subscribe([TOPIC_IN])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        if msg.error(): continue

        try:
            data = json.loads(msg.value().decode('utf-8'))
            email = data.get('email')
            subject = data.get('subject')
            body = data.get('body')

            print("\n" + "="*60, flush=True)
            print(f"📧 [EMAIL SENT] to: {email}", flush=True)
            print(f"   Subject: {subject}", flush=True)
            print(f"   Body: {body}", flush=True)
            print("="*60 + "\n", flush=True)

        except Exception as e:
            print(f"[Notifier] Errore: {e}", flush=True)
except KeyboardInterrupt:
    pass
finally:
    consumer.close()