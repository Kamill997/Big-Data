from database import init_db
import os
from flask import Flask,jsonify,request
import mysql.connector
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

@app.post("/register")
def register():
    data=request.json

    #id=data.get("id")
    email=data.get("email")
    name=data.get("name")
    surname=data.get("surname")

    if not email or not name or not surname:
        return jsonify({"error": "Inserire obblgiatoriamente tutti i campi"}), 400
    #Effettuo connessione al DB e poi creo oggetto "cursor" per poter fare le operazioni SQL
    db=connect_db()
    cursor = db.cursor()

    #cursor.execute("SELECT esito FROM user_requests WHERE id=%s", (id,))

    check_email= "SELECT email FROM users WHERE email=%s"
    cursor.execute(check_email,(email,))  #inserisco virgola perchè viene vista come stringa e non come lista

    if cursor.fetchone():
        return jsonify({"error": "Email esistente"}), 400

    query="""
    INSERT INTO users (email, name, surname)
    VALUES (%s, %s, %s) 
    """

    try:
        #Esegui la query e poi la aggiungo al db
        cursor.execute(query, (email, name, surname))
        db.commit()
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        #Chiudo le risorse aperte
        cursor.close()
        db.close()

    return jsonify({"message": "Utente registrato correttamente"})

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
    app.run(host="0.0.0.0", port=5000, debug=True)
    app.run(debug=True)