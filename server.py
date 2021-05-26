import grpc
from concurrent import futures
import time
import proto.raft_pb2_grpc as pb2_grpc
import proto.raft_pb2 as pb2
import sys


class RaftGRPC(pb2_grpc.LoggerServicer):

    def __init__(self, my_dict_address):
        self.items = []
        self.stub_list = []
        self.port_addr = []
        self.id = int(sys.argv[2])
        self.my_dict_address = my_dict_address
        self.my_dict_address.pop(str(self.id))

        for i in self.my_dict_address:
            tmp = i, self.my_dict_address[i]
            self.port_addr.append(tmp)

        for self.address in self.my_dict_address.values():
            print('{}:{}'.format("localhost", self.address.split(":")[1]))
            channel = grpc.insecure_channel(self.address)
            stub = pb2_grpc.LoggerStub(channel)
            self.stub_list.append(stub)

    def ListMessages(self, request, context):
        response = pb2.ListMessagesResponse(logs=self.items)
        return response

    def AppendMessage(self, request, context):
        item = request.log
        request = pb2.AppendMessageRequest(log=item)
        self.items.append(item)
        for stub in self.stub_list:
            response = stub.AppendMessage(request)
        return pb2.AppendMessageResponse()

    def get_log(self):
        return self.items

if __name__ == '__main__':

    my_dict_address = {}
    with open('servers.txt', 'r') as f:
        line = f.readline()
        while line:
            temp_list = line.split()
            print(temp_list)
            my_dict_address[temp_list[0]] = temp_list[1]
            line = f.readline()

    raftserver = RaftGRPC(my_dict_address)
    server = grpc.server(futures.ThreadPoolExecutor())
    pb2_grpc.add_LoggerServicer_to_server(raftserver, server)
    server.add_insecure_port('[::]:' + sys.argv[1])
    server.start()
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        server.stop(0)