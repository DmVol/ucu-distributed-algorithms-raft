from concurrent import futures
from states import Candidate, Follower, Leader
from os.path import isfile
from collections import OrderedDict
import random
import grpc
import logging
import time
import proto.raft_pb2_grpc as pb2_grpc
import proto.raft_pb2 as pb2
import sys
import signal

FOLLOWER = "follower"
CANDIDATE = "candidate"
LEADER = "leader"


class Server(pb2_grpc.RaftServicer):

    # Initialization
    def __init__(self, my_dict_address):
        self.term = 0
        self.voted_for = None
        self.log = OrderedDict()
        self.commit_index = 0
        self.role = Follower(self)
        self.current_role = FOLLOWER
        self.current_leader = 0
        self.votes_received = 0
        self.id = int(sys.argv[2])
        self.my_dict_address = my_dict_address
        self.my_dict_address.pop(str(self.id))
        self.last_log_index = 0
        self.last_log_term = 0
        self.timeout = time.time() + random.randint(5, 10)
        self.majority = (len(self.my_dict_address) + 1) // 2 + 1

        # Load data from log file if exists
        if (isfile('log.txt')):
            with open('log.txt', 'r') as fp:
                line = fp.readline()
                while (line):
                    temp_list = line.strip('\n').split(' ')
                    entry = pb2.LogEntry(term=int(temp_list[0]), command=str(temp_list[1]))
                    self.log[self.last_log_index + 1] = entry
                    self.last_log_index = len(self.log)
                    self.last_log_term = entry.term
                    self.term = entry.term
                    line = fp.readline()

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

    def get_log(self):
        return self.log

raft_server = None

# Write logs to file in a case of node termination

def terminate(signal,frame):
    raft_server.timeout = time.time() + 1000
    server.stop(0)
    print("Writing...")
    with open('log.txt', 'w') as f:
        log = raft_server.get_log()
        for entry in log.values():
            f.write(str(entry.term) + ' ' + str(entry.command) + '\n')


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, terminate)

    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    my_dict_address = {}
    with open('servers.txt', 'r') as f:
        line = f.readline()
        while line:
            temp_list = line.split()
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