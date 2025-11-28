import grpc
from concurrent import futures
import mysql.connector
import os
import user_service_pb2
import user_service_pb2_grpc

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_USER = "root"
DB_PASSWORD = "root"
DB_NAME = "user_db"

def connect_db():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

class UserService(user_service_pb2.UserServiceServicer):
    def verify_user(self,request,context):
        email=request.email

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

def server():
    server=grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    user_service_pb2_grpc.add_UserServiceServicer_to_server(UserService(), server)
    server.add_insecure_port('[::]:50051')
    print("gRPC running on port 50051")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    server()