import grpc
from concurrent import futures
from database import connect_db
import user_service_pb2
import user_service_pb2_grpc

#CLIENT
async def verify_email_grpc(email: str):
    try:
        async with grpc.aio.insecure_channel("container_user_manager:50051") as channel:
            stub = user_service_pb2_grpc.UserServiceStub(channel)
            requested = await stub.VerifyUser(user_service_pb2.UserRequest(email=email))
            return requested.exists
    except grpc.RpcError as e:
        return False
    except Exception as e:
        print(f"[gRPC Client Error] {e}")
        return False

#SERVER
class DataCollectorService(user_service_pb2_grpc.DataCollectorServiceServicer):
    def removeInterest(self, request, context):
        print(f"[gRPC Server] Richiesta rimozione totale interessi per: {request.email}")
        db = connect_db()
        cursor = db.cursor()

        try:
            cursor.execute("DELETE FROM user_interest WHERE email=%s", (request.email,))
            db.commit()
            success = True
        except Exception as e:
            print(f"[gRPC Server Error] {e}")
            success=False
        finally:
            cursor.close()
            db.close()

        return user_service_pb2.DeletedResponse(success=success)

def serve():
    port = '50051'
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    user_service_pb2_grpc.add_DataCollectorServiceServicer_to_server(DataCollectorService(), server)
    server.add_insecure_port('[::]:'+port)
    server.start()
    server.wait_for_termination()