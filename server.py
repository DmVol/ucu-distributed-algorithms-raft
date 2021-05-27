import grpc
from concurrent import futures
import time
import proto.raft_pb2_grpc as pb2_grpc
import proto.raft_pb2 as pb2
import sys


class RaftGRPC(pb2_grpc.RaftServicer):

    def __init__(self, my_dict_address):
        self.items = {}
        self.stub_list = []
        self.port_addr = []
        self.id = int(sys.argv[2])
        self.my_dict_address = my_dict_address
        self.my_dict_address.pop(str(self.id))
        self.term = 1
        self.leaderid = 1
        self.last_log_index = 0
        self.commit_index = 0

        for i in self.my_dict_address:
            tmp = i, self.my_dict_address[i]
            self.port_addr.append(tmp)

        for self.address in self.my_dict_address.values():
            print('{}:{}'.format("localhost", self.address.split(":")[1]))
            channel = grpc.insecure_channel(self.address)
            stub = pb2_grpc.RaftStub(channel)
            self.stub_list.append(stub)

    def ListMessages(self, request, context):
        print(self.items)
        response = pb2.ListMessagesResponse(logs=list(self.items.values()))
        return response

    def AppendMessage(self, request, context):
        if self.term < request.term:
            print("Term is lower tan in request")
            return pb2.ResponseAppendEntriesRPC(term=self.term, success=False)

        com = request.entry.command
        entry = pb2.LogEntry(term=self.term, command=com)
        self.items[self.last_log_index] = entry
        self.last_log_index += 1
        request = pb2.RequestAppendEntriesRPC(term=self.term, leaderId=self.id, prevLogIndex=self.last_log_index,
                                              prevLogTerm=0, entry=entry, leaderCommit=self.commit_index)
        if self.id == 1:
            for stub in self.stub_list:
                response = stub.AppendMessage(request)

        self.commit_index += 1

        return pb2.ResponseAppendEntriesRPC(term=self.term, success=True)

    def broadcast(self, request, context):
        if self.id == 1:
            for stub in self.stub_list:
                response = stub.AppendMessage(request)

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
    pb2_grpc.add_RaftServicer_to_server(raftserver, server)
    server.add_insecure_port('[::]:' + sys.argv[1])
    server.start()
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        server.stop(0)