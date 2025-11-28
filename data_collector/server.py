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

def connect_db():
    return mysql.connector.connect(
        host=DB_HOST,
        #port=3306,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )


async def verifica_email_grpc(email: str):
    try:
        async with grpc.aio.insecure_channel("container_user_manager:50051") as channel:
            stub = user_service_pb2_grpc.UserServiceStub(channel)
            richiesta = await stub.VerificaEmail(
                user_service_pb2.EmailDaVerificare(email=email)
            )
            return richiesta.esiste
    except grpc.RpcError as e:
        return False

@app.route('/sottoscriviInteresse', methods=['POST'])
async def sottoscriviInteresse():
    data=request.json
    email=data.get("email")
    if not email:
        return jsonify({"success": False,"message": "Email mancante"}), 400
    esitoVerifica=await verifica_email_grpc(email)
    if esitoVerifica:
        interessi=data.get("interessi")
        if not interessi:
            return jsonify({"success": False,"message": "Interesse mancante"}), 400
        db=connect_db()
        cursor=db.cursor()

        try:
            for interesse in interessi:
                insert="""INSERT INTO user_interest (email, airport_code) VALUES (%s, %s)"""
                cursor.execute(insert, (email, interesse))
            db.commit()
            return jsonify({"message": "Tutti gli interessi sono stati registrati"})
        except Exception as e:
            db.rollback()
            return jsonify({"error": f"Qualcosa è andato storto, interessi non registrati"}), 400
        finally:
            cursor.close()
            db.close()
    else:
        return jsonify({"error": f"L'email non è stata mai registrata"}), 400

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5000, debug=True)
