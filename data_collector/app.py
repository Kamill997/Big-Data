import os
import grpc
import asyncio
from database import init_db
from flask import Flask,request,jsonify
import user_service_pb2
import user_service_pb2_grpc
import mysql.connector
import json

app = Flask(__name__)

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_USER = "root"
DB_PASSWORD = "root"
DB_NAME = "data_db"

OPENSKY_API_URL = "https://opensky-network.org/api/flights"
OPENSKY_USER = os.getenv('OPENSKY_USER')
OPENSKY_PASS = os.getenv('OPENSKY_PASSWORD')

def connect_db():
    return mysql.connector.connect(
        host=DB_HOST,
        #port=3306,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )


async def verify_email_grpc(email: str):
    try:
        async with grpc.aio.insecure_channel("container_user_manager:50051") as channel:
            stub = user_service_pb2_grpc.UserServiceStub(channel)
            requested = await stub.UserRequest(
                user_service_pb2.UserRequest(email=email)
            )
            return requested.exists
    except grpc.RpcError as e:
        return False

def airports_flights(airport_code,start_time,end_time):
    conn=connect_db()
    cursor=conn.cursor()

    auth=(OPENSKY_USER,OPENSKY_PASS) if OPENSKY_USER and OPENSKY_PASS else None


#API
@app.post('/sottoscriviInteresse')
async def sottoscriviInteresse():
    data=request.json
    email=data.get("email")
    if not email:
        return jsonify({"success": False,"message": "Email mancante"}), 400
    esitoVerifica=await verify_email_grpc(email)
    if esitoVerifica:
        interessi=data.get("interessi")
        if not interessi:
            return jsonify({"success": False,"message": "Interesse mancante"}), 400
        db=connect_db()
        cursor=db.cursor()
        risultato_operazione = []
        try:
            for interesse in interessi:
                cursor.execute("SELECT * FROM user_interest WHERE airport_code=%s and email=%s", (interesse,email))
                if cursor.fetchone() is None:
                    insert="""INSERT INTO user_interest (email, airport_code) VALUES (%s, %s)"""
                    cursor.execute(insert, (email, interesse))
                    risultato_operazione.append({"message":f"L'interesse {interesse} è stato registrato correttamente"})
                else:
                    risultato_operazione.append({"message":f"L'interesse {interesse} era stato già indicato"})
            db.commit()
            return jsonify({"Esito operazione": risultato_operazione})
        except Exception as e:
            db.rollback()
            return jsonify({"error": f"Qualcosa è andato storto, interessi non registrati"}), 400
        finally:
            cursor.close()
            db.close()
    else:
        return jsonify({"error": f"L'email non è stata mai registrata"}), 422

@app.delete('/eliminaInteresse')
async def eliminaInteresse():
    data=request.json
    email=data.get("email")
    if email is None:
        return jsonify({"Error":"Non hai indicato un email"}), 400
    esitoVerifica=await verify_email_grpc(email)
    if esitoVerifica:
        interessi=data.get("interessi")
        if not interessi:
            return jsonify({"Error":"Non hai indicato un interesse"}), 400
        db=connect_db()
        cursor=db.cursor()
        risultato_operazione = []
        try:
            for interesse in interessi:
                cursor.execute("SELECT * FROM user_interest WHERE email=%s and airport_code=%s", (email,interesse))
                if cursor.fetchone():
                    cursor.execute("DELETE FROM user_interest WHERE email=%s AND airport_code=%s", (email,interesse))
                    risultato_operazione.append({"Message":f"L'interesse {interesse} è stato eliminato"})
                else:
                    risultato_operazione.append({"Message": f"L'interesse {interesse} non è presente"})
            db.commit()
            return jsonify({"Esito operazione": risultato_operazione})
        except Exception as e:
            db.rollback()
            return jsonify({"Error":f"Qualcosa è andato storto e gli interessi non sono stati eliminati"})
        finally:
            cursor.close()
            db.close()
    else:
        return jsonify({"Error":"Email non registrata"}),422

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5000, debug=True)