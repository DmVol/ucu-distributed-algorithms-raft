import random
import grpc
import logging
from concurrent import futures
import time
import proto.raft_pb2_grpc as pb2_grpc
import sys
from states import Candidate, Follower, Leader

FOLLOWER = "follower"
CANDIDATE = "candidate"
LEADER = "leader"


class Server(pb2_grpc.RaftServicer):

    # Initialization
    def __init__(self, my_dict_address):
        self.term = 0
        self.voted_for = None
        self.log = {}
        self.commit_index = 0
        self.role = Follower(self)
        self.current_role = FOLLOWER
        self.current_leader = 0
        self.votes_received = 0
        self.stub_list = []
        self.port_addr = []
        self.id = int(sys.argv[2])
        self.my_dict_address = my_dict_address
        self.my_dict_address.pop(str(self.id))
        self.last_log_index = 0
        self.last_log_term = 0
        self.timeout = time.time() + random.randint(5, 10)
        self.timeout_thread = None
        self.majority = (len(self.my_dict_address) + 1) // 2 + 1

        # Form nodes address list
        for i in self.my_dict_address:
            tmp = i, self.my_dict_address[i]
            self.port_addr.append(tmp)

        for self.address in self.my_dict_address.values():
            # print('{}:{}'.format(self.address.split(":")[0], self.address.split(":")[1]))
            channel = grpc.insecure_channel(self.address)
            stub = pb2_grpc.RaftStub(channel)
            self.stub_list.append(stub)

        logging.info(f"Node {self.id} initialized")

    def become(self, state):
        self.role = state(self)
        self.refresh()

    def refresh(self):
        self.role.run()

    def Vote(self, request, context):
        logging.info(f"Node {self.id} received vote request")
        response = self.role.vote(request, context)
        logging.info(f"Node {self.id} voted - vote granted: {response.voteGranted}")
        return response

    def ListMessages(self, request, context):
        logging.info(f"Node {self.id} received list messages request")
        response = self.role.list_messages(request)
        return response

    def AppendMessage(self, request, context):
        logging.info(f"Node {self.id} append message request")
        response = self.role.append(request)
        return response


def serve():
    my_dict_address = {}
    with open('servers.txt', 'r') as f:
        line = f.readline()
        while line:
            temp_list = line.split()
            # print(temp_list)
            my_dict_address[temp_list[0]] = temp_list[1]
            line = f.readline()

    raft_server = Server(my_dict_address)
    server = grpc.server(futures.ThreadPoolExecutor())
    pb2_grpc.add_RaftServicer_to_server(raft_server, server)

    server.add_insecure_port('[::]:' + sys.argv[1])
    server.start()
    try:
        while True:
            time.sleep(1)
            raft_server.refresh()
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)
    serve()