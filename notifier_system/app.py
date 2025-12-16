import json
import os
import sys
import smtplib
from email.message import EmailMessage
from confluent_kafka import Consumer

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'broker_kafka:9092')
TOPIC_IN = 'to-notifier'

consumer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'notifier_group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

SMTP_HOST = os.getenv('SMTP_HOST', 'smtp.gmail.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', 587))
SMTP_USER = os.getenv('SMTP_USER')
SMTP_PASSWORD = os.getenv('SMTP_PASSWORD')

consumer = Consumer(consumer_conf)
consumer.subscribe([TOPIC_IN])

def send_email(receiver_email, subject, body):
    if not SMTP_USER or not SMTP_PASSWORD:
        print("[Notifier] ERRORE: Credenziali SMTP mancanti!", flush=True)
        return

    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = SMTP_USER
    msg['To'] = receiver_email

    try:
        # Connessione al server SMTP
        print(f"[Notifier] Connessione a {SMTP_HOST}:{SMTP_PORT}", flush=True)
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()  # Upgrade della connessione a sicura
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)

        print(f"[Notifier] Email inviata con successo a {receiver_email}", flush=True)
    except Exception as e:
        print(f"[Notifier] Errore invio email: {e}", flush=True)


try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None: continue
        if msg.error():
            print(f"Consumer error: {msg.error()}", flush=True)
            continue

        try:
            data = json.loads(msg.value().decode('utf-8'))
            email = data.get('email')
            subject = data.get('subject')
            body = data.get('body')

            print(f"[Notifier] Ricevuto ordine invio per: {email}", flush=True)

            send_email(email, subject, body)

            consumer.commit(asynchronous=False)

        except Exception as e:
            print(f"[Notifier] Errore: {e}", flush=True)
except KeyboardInterrupt:
    pass
finally:
    consumer.close()

