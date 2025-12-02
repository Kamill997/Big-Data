import threading
import time
from database import init_db
from flask import Flask,jsonify,request
from user_logic import UserLogic
from gRPC_Logic import serve
app = Flask(__name__)

@app.post("/register")
def register():
    data=request.json
    id=data.get("id")
    email=data.get("email")
    name=data.get("name")
    surname=data.get("surname")

    if not email or not name or not surname or not id:
        return jsonify({"error": f"Inserire obblgiatoriamente tutti i campi"}), 400

    success, message, status = UserLogic.register(
        id, email, name, surname
    )
    if success:
        return jsonify({"message": message}), status
    else:
        return jsonify({"error": message}), status


@app.delete("/delete")
def delete():
    data=request.json
    email=data.get("email")

    if not email:
        return jsonify({"error": "Email obbligatoria"}), 400

    success, message, status = UserLogic.delete_user(email)

    if success:
        return jsonify({"message": message}), status
    else:
        return jsonify({"error": message}), status


if __name__ == "__main__":
    time.sleep(5)
    init_db()
    grpc_thread = threading.Thread(target=serve, daemon=True)
    grpc_thread.start()
    app.run(host="0.0.0.0", port=5000, debug=True)
