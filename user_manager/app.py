import threading
from database import init_db
import os
from flask import Flask,jsonify,request
import mysql.connector
import grpc
import user_service_pb2, user_service_pb2_grpc
from concurrent import futures

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

@app.get("/")
def index():
    return "User Manager Service is running"


class UserService(user_service_pb2_grpc.UserServiceServicer):
    def VerificaEmail(self, request, context):
        self.db = connect_db()
        email = request.email
        cursor = self.db.cursor()
        cursor.execute("SELECT * FROM users WHERE email=%s", (email,))

        if cursor.fetchone() is not None:
            self.db.close()
            return user_service_pb2.EsitoVerifica(
                esiste=True,
            )
        else:
            self.db.close()
            return user_service_pb2.EsitoVerifica(
                esiste=False,

            )

def serve():
    port = '50051'
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    user_service_pb2_grpc.add_UserServiceServicer_to_server(UserService(), server)
    server.add_insecure_port('[::]:'+port)  # porta gRPC interna
    server.start()
    print("[USER_SERVICE] Server gRPC avviato sulla porta 50051")
    server.wait_for_termination()

@app.route('/registrazione', methods=['POST'])
def register():
    data=request.json
    id=data.get("id")
    email=data.get("email")
    name=data.get("nome")
    surname=data.get("cognome")
    if not email or not name or not surname or not id:
        return jsonify({"error": "Inserire tutti i campi"}), 400
    db=connect_db()
    cursor = db.cursor()

    cursor.execute("SELECT esito_richiesta FROM requestId WHERE id=%s", (id,))
    esito_precedente=cursor.fetchone()
    if esito_precedente is not None: #ho già elaborato la richiesta
        cursor.close()
        db.close()
        return jsonify({"error": f"Richiesta già elaborata precedentemente. L'esito è stato:{esito_precedente[0]}"}), 400

    #Verifica dell' email duplicata ( solo se la richiesta è NUOVA)
    cursor.execute("SELECT * FROM users WHERE email=%s", (email,))
    if cursor.fetchone():
        esito="Utente già registrato"
        query="""
              INSERT INTO requestId(id,esito_richiesta) 
              VALUES(%s,%s)
              """
        try:
            cursor.execute(query,(id,esito))
            db.commit()
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        finally:
            cursor.close()
            db.close()
            return jsonify({"error": "Utente già registrato"}), 400

    try:
        insert_user_query = """
                            INSERT INTO users (email, name, surname)
                            VALUES (%s, %s, %s)
                            """
        cursor.execute(insert_user_query, (email, name, surname))
        esito="Utente registrato correttamente"
        insert_id_query = """
                          INSERT INTO requestId (id, esito_richiesta)
                          VALUES (%s, %s)
                          """
        cursor.execute(insert_id_query, (id, esito))
        db.commit()
        return jsonify({"message": esito}), 200
    except Exception as e:
        db.rollback()
        return jsonify({"error": str(e)}), 500

    finally:
            cursor.close()
            db.close()



@app.route('/rimozione', methods=['DELETE'])
def rimozione():
    email=request.json.get("email")
    if not email:
        return jsonify({"error": "Email non passata"}), 400
    else:
        db=connect_db()
        cursor = db.cursor()
        cursor.execute("SELECT * from users where email=%s", (email,))
        if cursor.fetchone() is None:
            jsonify("Email non trovata")
            cursor.close()
            db.close()
            return jsonify({"message": "Utente non trovato"}), 400
        else:
            try:
                cursor.execute("DELETE FROM users WHERE email=%s", (email,))
                db.commit()
            except Exception as e:
                return jsonify({"error": str(e)}), 500
            finally:
                cursor.close()
                db.close()
                return jsonify({"message": "Utente cancellato correttamente"})

if __name__ == "__main__":
    init_db()
    # Avvio gRPC in thread separato
    grpc_thread = threading.Thread(target=serve, daemon=True)
    grpc_thread.start()
    app.run(host="0.0.0.0", port=5000, debug=True)
