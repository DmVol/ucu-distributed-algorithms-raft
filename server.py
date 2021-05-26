import grpc
from concurrent import futures
import time
import proto.api_pb2_grpc as pb2_grpc
import proto.api_pb2 as pb2


class GrpcLogger(pb2_grpc.LoggerServicer):

    def __init__(self):
        self.items = []

        self.host = 'secondary-1'
        self.server_port = 50052

        # instantiate a channel
        self.channel = grpc.insecure_channel(
            '{}:{}'.format(self.host, self.server_port))

        # bind the client and the server
        self.stub = pb2_grpc.LoggerStub(self.channel)

    def ListMessages(self, request, context):
        response = pb2.ListMessagesResponse(logs=self.items)
        return response

    def AppendMessage(self, request, context):
        item = request.log
        request = pb2.AppendMessageRequest(log=item)
        self.stub.AppendMessage(request)
        self.items.append(item)
        return pb2.AppendMessageResponse()

if __name__ == '__main__':
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_LoggerServicer_to_server(GrpcLogger(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        server.stop(0)
