import os
import mysql.connector
import threading
import grpc
import user_service_pb2
import user_service_pb2_grpc
from concurrent import futures
from database import init_db
from flask import Flask,jsonify,request

app = Flask(__name__)

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_USER = "root"
DB_PASSWORD = "root"
DB_NAME = "user_db"

# Connessione al DB
def connect_db():
    return mysql.connector.connect(
        host=DB_HOST,
        #port=3306,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
)

class UserService(user_service_pb2_grpc.UserServiceServicer):
    def VerifyUser(self,request,context):
        email=request.email
        print(f"[gRPC] Richiesta verifica per: {email}")

        try:
            db=connect_db()
            cursor = db.cursor()
            cursor.execute("select email from users where email=%s", (email,))
            exists = cursor.fetchone() is not None

            cursor.close()
            db.close()

            return user_service_pb2.UserResponse(exists=exists)

        except Exception as e:
            print(f"[gRPC Error] {e}")
            return user_service_pb2.UserResponse(exists=False)

async def removeInterest(email: str):
    try:
        async with grpc.aio.insecure_channel("container_data_collector:50051") as channel:
            stub = user_service_pb2_grpc.UserServiceStub(channel)
            request = await stub.EliminaInteresse(
                user_service_pb2.EmailDaVerificare(email=email)
            )
            return request.UserResponse
    except grpc.RpcError as e:
        return False, str(e)

def server():
    server=grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    user_service_pb2_grpc.add_UserServiceServicer_to_server(UserService(), server)
    server.add_insecure_port('[::]:50051')
    print("gRPC running on port 50051")
    server.start()
    server.wait_for_termination()

@app.post("/register")
def register():
    data=request.json

    id=data.get("id")
    email=data.get("email")
    name=data.get("name")
    surname=data.get("surname")

    if not email or not name or not surname or not id:
        return jsonify({"error": "Inserire obblgiatoriamente tutti i campi"}), 400

    db=connect_db()
    cursor = db.cursor()

    #Controllo se una data richiesta è già presente
    cursor.execute("SELECT esito_richiesta FROM requestID WHERE id=%s", (id,))
    esito=cursor.fetchone()

    #verifica sulla richiesta per vedere se è stata elaborata correttamente
    if esito is not None:
        cursor.close()
        db.close()
        return jsonify({"error": f"Richiesta già elaborata precedentemente. \n Esito:{esito[0]}"}), 400

    #verifica sulla mail (Solo se viene effettuta una nuova richiesta)
    cursor.execute("SELECT email FROM users WHERE email=%s",(email,))  #inserisco virgola perchè viene vista come stringa e non come lista

    if cursor.fetchone():
        esito="Utente già registrato"
        insert_request="""
                      INSERT INTO requestID (id,esito_richiesta)
                      VALUES (%s, %s)
                      """

        try:
            cursor.execute(insert_request,(id,esito))
            db.commit()
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        finally:
            cursor.close()
            db.close()
            return jsonify({"error": "Utente già registrato"}), 400

    try:
        insert_user = """
                            INSERT INTO users (email, name, surname)
                            VALUES (%s, %s, %s)
                            """
        cursor.execute(insert_user, (email, name, surname))
        esito="Utente registrato correttamente"

        insert_newRequest = """
                          INSERT INTO requestID (id, esito_richiesta)
                          VALUES (%s, %s)
                          """
        cursor.execute(insert_newRequest, (id, esito))
        db.commit()
        return jsonify({"message": esito}), 200
    except Exception as e:
        db.rollback()
        return jsonify({"error": str(e)}), 500

    finally:
        cursor.close()
        db.close()


@app.delete("/delete")
def delete():
    data=request.json

    email=data.get("email")

    if not email:
        return jsonify({"error": "Email obbligatorie"}), 400

    db=connect_db()
    cursor = db.cursor()

    check_user= "SELECT email FROM users WHERE email=%s"

    cursor.execute(check_user,(email,))
    if cursor.fetchone():
        query="""
              DELETE FROM users WHERE email=%s
              """
        try:
            #Esegui la query e poi la aggiungo al db
            cursor.execute(query, (email,))
            db.commit()
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        finally:
            #Chiudo le risorse aperte
            cursor.close()
            db.close()

        return jsonify({"message": "Utente eliminato correttamente"})
    else:
        return jsonify({"message": "Nessun utente con tale email presente"})


if __name__ == "__main__":
    init_db()
    grpc_thread = threading.Thread(target=server, daemon=True)
    grpc_thread.start()
    app.run(host="0.0.0.0", port=5000, debug=True)
    #app.run(debug=True)