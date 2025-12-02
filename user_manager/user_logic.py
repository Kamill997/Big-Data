from database import connect_db
import asyncio
from gRPC_Logic import removeInterest

class UserLogic:

    @staticmethod
    def register(id, email, name, surname):
        db = connect_db()
        cursor = db.cursor()

        try:
            cursor.execute("SELECT esito_richiesta FROM requestID WHERE id=%s", (id,))

            request_row = cursor.fetchone()
            if request_row is not None:
                return False, f"Richiesta già elaborata precedentemente. Esito: {request_row[0]}", 400

            # Controllo se l'utente esiste già
            cursor.execute("SELECT email FROM users WHERE email=%s", (email,))
            if cursor.fetchone():
                esito = "Utente già registrato"
                cursor.execute(
                    "INSERT INTO requestID (id, esito_richiesta) VALUES (%s, %s)",
                    (id, esito)
                )
                db.commit()
                return False, esito, 400

            # Inserimento Utente
            cursor.execute(
                "INSERT INTO users (email, name, surname) VALUES (%s,%s,%s)",
                (email, name, surname)
            )

            #Inserimento Richiesta
            esito = "Utente registrato correttamente"
            cursor.execute(
                "INSERT INTO requestID (id, esito_richiesta) VALUES (%s,%s)",
                (id,esito )
            )

            db.commit()
            return True, esito , 200

        except Exception as e:
            db.rollback()
            return False, str(e), 500

        finally:
            cursor.close()
            db.close()

    @staticmethod
    def delete_user(email):
        db = connect_db()
        cursor = db.cursor()

        try:
            cursor.execute("SELECT * FROM users WHERE email=%s", (email,))
            if not cursor.fetchone():
                return False, "Nessun utente trovato con tale email", 404

            try:
                # Elimino interessi con gRPC
                success = asyncio.run(removeInterest(email))
                if not success:
                    return False, "Impossibile eliminare gli interessi, uente non eliminato", 500
            except Exception as e:
                return False, f"Errore comunicazione gRPC: {str(e)}", 500

            cursor.execute("DELETE FROM users WHERE email=%s", (email,))
            db.commit()

            return True, "Utente eliminato correttamente",200

        except Exception as e:
            db.rollback()
            return False, str(e),500

        finally:
            cursor.close()
            db.close()