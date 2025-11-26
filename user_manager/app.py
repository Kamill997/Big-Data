from flask import Flask,request,jsonify
import mysql.connector
app = Flask(__name__)


def connect_db():
    return mysql.connector.connect(
        host="localhost",
        port=3306,
        user="root",
        password="root",
        database="user_db"
    )

@app.route('/registrazione', methods=['POST'])
def register():
    data=request.json
    id=data.get("id")
    email=data.get("email")
    name=data.get("nome")
    surname=data.get("cognome")
    print(id)
    if not email or not name or not surname:
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
                          VALUES (%s, %s) \
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
    app.run(debug=True)