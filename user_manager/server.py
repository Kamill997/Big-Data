import mysql.connector

# Connessione al DB
conn = mysql.connector.connect(
    host="localhost",   # o IP del container se usi network docker
    port=3306,
    user="root",
    password="root",
    database="user_db"
)

cursor = conn.cursor(dictionary=True)

# Eseguo la query
cursor.execute("SELECT * FROM users LIMIT 10")

# Recupero i risultati
rows = cursor.fetchall()

# Stampo i dati
for row in rows:
    print(row)

cursor.close()
conn.close()