import random
import grpc
from concurrent import futures
import time
import proto.raft_pb2_grpc as pb2_grpc
import proto.raft_pb2 as pb2
import sys


class RaftGRPC(pb2_grpc.RaftServicer):

    # Initialization
    def __init__(self, my_dict_address):
        self.term = 0
        self.voted_for = None
        self.log = {}
        self.commit_index = 0
        self.current_role = 'follower'
        self.current_leader = 1 # None
        self.votes_received = 0
        self.stub_list = []
        self.port_addr = []
        self.id = int(sys.argv[2])
        self.my_dict_address = my_dict_address
        self.my_dict_address.pop(str(self.id))
        self.last_log_index = 0
        self.last_log_term = 0
        self.timeout = time.time() + random.randint(5, 10)

        for i in self.my_dict_address:
            tmp = i, self.my_dict_address[i]
            self.port_addr.append(tmp)

        for self.address in self.my_dict_address.values():
            print('{}:{}'.format("localhost", self.address.split(":")[1]))
            channel = grpc.insecure_channel(self.address)
            stub = pb2_grpc.RaftStub(channel)
            self.stub_list.append(stub)

    def refresh(self):
        if self.current_role == "follower":
            if time.time() > self.timeout:
                print(f"Timeout reached for node {self.id} - becoming candidate")
                self.current_role = "candidate"
        elif self.current_role == "candidate":
            if time.time() > self.timeout:
                self.term += 1
                self.votes_received = 1
                print(f"Node {self.id} start sending vote requests")
                request = pb2.RequestVoteRPC(term=self.term, candidateId=self.id, lastLogIndex=self.last_log_index,
                                             lastLogTerm=self.last_log_term)

                responses = 0
                for stub in self.stub_list:
                    try:
                        response = stub.Vote(request)
                        if response.voteGranted:
                            self.votes_received += 1
                    except:
                        print("connection error")
                    responses += 1
                self.timeout = time.time() + random.randint(5, 10)
            elif self.votes_received >= (len(self.my_dict_address) + 1) // 2 + 1:
                print("becoming leader")
                self.current_role = "leader"
                self.votes_received = 1
                self.voted_for = self.id
        elif self.current_role == "leader":
            print("Node is leader")
            if time.time() > self.timeout:
                print("Start sending requests")
                prevLogIndex = self.last_log_index
                if prevLogIndex in self.log:
                    entry = self.log[prevLogIndex]
                else:
                    entry = None
                # Append Entries Request.
                request = pb2.RequestAppendEntriesRPC(term=self.term, leaderId=self.id, prevLogIndex=prevLogIndex,
                                                      prevLogTerm=self.last_log_term, entry=entry, leaderCommit=self.commit_index)
                responses = 0
                for stub in self.stub_list:
                    print(responses)
                    try:
                        response = stub.AppendMessage(request)
                        while response.success == False:
                            prevLogIndex -= 1
                            entry = self.log[prevLogIndex]
                            request = pb2.RequestAppendEntriesRPC(term=self.term, leaderId=self.id,
                                                                  prevLogIndex=prevLogIndex, prevLogTerm=entry.term,
                                                                  entry=entry, leaderCommit=self.commit_index)
                            response = stub.AppendMessage(request)
                        while prevLogIndex < self.last_log_index:
                            prevLogIndex += 1
                            entry = self.log[prevLogIndex]
                            request = pb2.RequestAppendEntriesRPC(term=self.term, leaderId=self.id,
                                                                  prevLogIndex=prevLogIndex, prevLogTerm=entry.term,
                                                                  entry=entry, leaderCommit=self.commit_index)
                            response = stub.AppendMessage(request)
                    except:
                        print('cannot connect to ' + str(self.port_addr[responses]))
                    responses += 1
                self.timeout = time.time() + 1


    def Vote(self, request, context):
        print(f"Node {self.id} received vote request")
        term_ok, log_ok = None, None

        # Check log
        if request.lastLogTerm > self.last_log_term:
            log_ok = True
        elif request.lastLogTerm == self.last_log_term and request.lastLogIndex >= self.last_log_index:
            log_ok = True
        else:
            log_ok = False

        # Check term
        if request.term > self.term:
            term_ok = True
        elif request.term == self.term and self.voted_for in (None, request.candidateId):
            term_ok = True
        else:
            term_ok = False

        # Return response
        if term_ok and log_ok:
            print("Vote granted")
            self.term = request.term
            self.current_role = "follower"
            self.voted_for = request.candidateId
            return pb2.ResponseVoteRPC(term=self.term, voteGranted=True)
        else:
            print("Vote rejected")
            return pb2.ResponseVoteRPC(term=self.term, voteGranted=False)

    def ListMessages(self, request, context):
        print(self.log)
        response = pb2.ListMessagesResponse(logs=list(self.log.values()))
        return response

    def AppendMessage(self, request, context):
        if self.term < request.term:
            print("Term is lower tan in request")
            return pb2.ResponseAppendEntriesRPC(term=self.term, success=False)
        self.timeout = time.time() + random.randint(5, 10)
        com = request.entry.command
        entry = pb2.LogEntry(term=self.term, command=com)
        self.log[self.last_log_index] = entry
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
        return self.log

if __name__ == '__main__':

    my_dict_address = {}
    with open('servers.txt', 'r') as f:
        line = f.readline()
        while line:
            temp_list = line.split()
            print(temp_list)
            my_dict_address[temp_list[0]] = temp_list[1]
            line = f.readline()

    raft_server = RaftGRPC(my_dict_address)
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