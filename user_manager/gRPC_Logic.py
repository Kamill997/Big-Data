import grpc
from concurrent import futures
from database import connect_db
import user_service_pb2
import user_service_pb2_grpc

#CLIENT
async def removeInterest(email: str):
    try:
        async with grpc.aio.insecure_channel("container_data_collector:50051") as channel:
            stub = user_service_pb2_grpc.DataCollectorServiceStub(channel)
            request = await stub.removeInterest(
                user_service_pb2.UserRequest(email=email)
            )
            return request.success
    except grpc.RpcError as e:
        return False, str(e)

#SERVER
class UserService(user_service_pb2_grpc.UserServiceServicer):
    def VerifyUser(self,request,context):
        email=request.email
        print(f"[gRPC] Richiesta verifica per: {email}")
        try:
            self.db=connect_db()
            cursor = self.db.cursor()
            cursor.execute("select email from users where email=%s", (email,))
            if cursor.fetchone() is not None:
                cursor.close()
                self.db.close()
                return user_service_pb2.UserResponse(exists=True)
            else:
                cursor.close()
                self.db.close()
                return user_service_pb2.UserResponse(exists=False)
        except Exception as e:
            print(f"[gRPC Error] {e}")
            return user_service_pb2.UserResponse(exists=False)

def serve():
    port = '50051'
    server=grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    user_service_pb2_grpc.add_UserServiceServicer_to_server(UserService(), server)
    server.add_insecure_port('[::]:'+ port)
    print(f"gRPC running on port {port}")
    server.start()
    server.wait_for_termination()