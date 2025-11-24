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

    email=data.get("email")
    nome=data.get("nome")
    cognome=data.get("cognome")

    if not email or not nome or not cognome:
        return jsonify({"error": "Inserire tutti i campi"}), 400

    db=connect_db()
    cursor = db.cursor()
    cursor.execute("SELECT * FROM utenti WHERE email=%s", (email,))
    if cursor.fetchone():
        return jsonify({"error": "Email già presente"}), 400
    else:
        query="""
              INSERT INTO utenti (email, nome, cognome)
              VALUES (%s, %s, %s)
              """
        try:
            cursor.execute(query, (email, nome, cognome))
            db.commit()
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        finally:
            cursor.close()
            db.close()
        return jsonify({"message": "Utente registrato correttamente"})

@app.route('/rimozione', methods=['DELETE'])
def rimozione():
    email=request.json.get("email")
    if not email:
        return jsonify({"error": "Email non passata"}), 400
    else:
        db=connect_db()
        cursor = db.cursor()
        cursor.execute("SELECT * from utenti where email=%s", (email,))
        if cursor.fetchone() is None:
            jsonify("Email non trovata")
            cursor.close()
            db.close()
            return jsonify({"message": "Utente non trovato"}), 400
        else:
            try:
                cursor.execute("DELETE FROM utenti WHERE email=%s", (email,))
                db.commit()
            except Exception as e:
                return jsonify({"error": str(e)}), 500
            finally:
                cursor.close()
                db.close()
                return jsonify({"message": "Utente cancellato correttamente"})





if __name__ == "__main__":
    app.run(debug=True)