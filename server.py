import random
import grpc
from queue import Queue
from concurrent import futures
import time
import container.proto.raft_pb2_grpc as pb2_grpc
import container.proto.raft_pb2 as pb2
import sys
import threading

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
            print('{}:{}'.format("localhost", self.address.split(":")[1]))
            channel = grpc.insecure_channel(self.address)
            stub = pb2_grpc.RaftStub(channel)
            self.stub_list.append(stub)

    def reset_timeout(self):
        self.timeout = time.time() + random.randint(5, 10)

    def init_timeout(self):
        self.reset_timeout()
        # safety guarantee, timeout thread may expire after election
        if self.timeout_thread and self.timeout_thread.isAlive():
            return
        self.timeout_thread = threading.Thread(target=self.timeout_loop)
        self.timeout_thread.start()

    def timeout_loop(self):
        # only stop timeout thread when winning the election
        while self.current_role != LEADER:
            delta = self.timeout - time.time()
            if delta < 0:
                self.current_role = CANDIDATE
            else:
                time.sleep(delta)

    def ask_vote(self, request, stub):
        try:
            response = stub.Vote(request)
            if response.voteGranted:
                self.votes_received += 1

        except:
            print("connection error")

    def broadcast(self, node_id, prevLogIndex, request, stub):
        if prevLogIndex in self.log:
            entry = self.log[prevLogIndex]
        else:
            entry = None
        try:
            response = stub.AppendMessage(request)
            print(response)
            while response.success == False:
                if prevLogIndex >= 1:
                    prevLogIndex -= 1
                print(f"sending entry with index -  index no {prevLogIndex}, current index is {self.last_log_index}")
                print(entry)
                entry = self.log[prevLogIndex]
                request = pb2.RequestAppendEntriesRPC(term=self.term, leaderId=self.id,
                                                      prevLogIndex=prevLogIndex, prevLogTerm=entry.term,
                                                      entry=entry, leaderCommit=self.commit_index)
                response = stub.AppendMessage(request)

            while prevLogIndex < self.last_log_index:
                prevLogIndex += 1
                print(f"sending entry with index -  index no {prevLogIndex}, current index is {self.last_log_index}")
                print(entry)
                entry = self.log[prevLogIndex]
                request = pb2.RequestAppendEntriesRPC(term=self.term, leaderId=self.id,
                                                      prevLogIndex=prevLogIndex, prevLogTerm=entry.term,
                                                      entry=entry, leaderCommit=self.commit_index)
                response = stub.AppendMessage(request)

            return response

        except Exception as e:
            print(e)
            print('cannot connect to ' + str(self.port_addr[node_id]))

    def follower_run(self):
        if time.time() > self.timeout:
            print(f"Timeout reached for node {self.id} - becoming candidate")
            self.current_role = CANDIDATE

    def candidate_run(self):
        if time.time() > self.timeout:
            self.term += 1
            self.votes_received = 1
            print(f"Node {self.id} start sending vote requests")
            request = pb2.RequestVoteRPC(term=self.term, candidateId=self.id, lastLogIndex=self.last_log_index,
                                         lastLogTerm=self.last_log_term)

            barrier = threading.Barrier(self.majority - 1, timeout=2)
            for stub in self.stub_list:
                threading.Thread(target=self.ask_vote,
                                 args=(request, stub)).start()

            barrier.wait()
            print(self.votes_received)
            self.reset_timeout()
        elif self.votes_received >= self.majority:
            print("becoming leader")
            self.current_role = LEADER
            self.votes_received = 1
            self.voted_for = self.id
            self.timeout = time.time()

    def leader_run(self):
        if time.time() > self.timeout:
            print("Start sending requests")
            prevLogIndex = self.last_log_index
            if prevLogIndex in self.log:
                entry = self.log[prevLogIndex]
            else:
                entry = None
            # Append Entries Request.
            print(f"my term is {self.term} last log term is {self.last_log_term}")
            request = pb2.RequestAppendEntriesRPC(term=self.term, leaderId=self.id, prevLogIndex=prevLogIndex,
                                                  prevLogTerm=self.last_log_term, entry=entry,
                                                  leaderCommit=self.commit_index)

            for node_id, stub in enumerate(self.stub_list):
                threading.Thread(target=self.broadcast,
                                 args=(node_id, prevLogIndex, request, stub)).start()

            self.timeout = time.time() + 1

    def refresh(self):

        if self.current_role == FOLLOWER:
            print(f"{self.id} is follower")
            self.follower_run()

        elif self.current_role == CANDIDATE:
            print(f"{self.id} is candidate")
            self.candidate_run()

        elif self.current_role == LEADER:
            print(f"{self.id} is leader")
            self.leader_run()


    def Vote(self, request, context):
        print(f"Node {self.id} received vote request")
        term_ok, log_ok = False, False

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
            self.current_role = FOLLOWER
            self.voted_for = request.candidateId
            self.reset_timeout()
            return pb2.ResponseVoteRPC(term=self.term, voteGranted=True)
        else:
            print("Vote rejected")
            return pb2.ResponseVoteRPC(term=self.term, voteGranted=False)

    def ListMessages(self, request, context):
        print(self.log)
        print(self.commit_index)
        response = pb2.ListMessagesResponse(logs=list(self.log.values()))
        return response

    def become_follower(self, request):
        print("term is greater than mine")
        self.term = request.term
        self.voted_for = -1
        self.current_role = FOLLOWER
        self.current_leader = request.leaderId
        self.reset_timeout()
        return pb2.ResponseAppendEntriesRPC(term=self.term, success=True)

    def leader_append(self, request):
        entry = pb2.LogEntry(term=self.term, command=request.entry.command)
        self.last_log_term = self.term
        self.last_log_index += 1
        self.log[self.last_log_index] = entry

        req = pb2.RequestAppendEntriesRPC(term=self.term, leaderId=self.id, prevLogIndex=self.last_log_index,
                                          prevLogTerm=self.last_log_term, entry=entry,
                                          leaderCommit=self.commit_index)

        # Create objects for threads result handling
        que = Queue()
        thread_list = []
        responses = []

        for node_id, stub in enumerate(self.stub_list):
            t = threading.Thread(target=lambda q, arg1, arg2, arg3, arg4: q.put(self.broadcast(arg1, arg2, arg3, arg4)),
                                 args=(que, node_id, self.last_log_index, req, stub))
            t.start()
            thread_list.append(t)

        for t in thread_list:
            t.join()

        while not que.empty():
            result = que.get()
            if result:
                responses.append(result.success)
            print(responses, responses.count(True))

        # Count all approves
        if responses.count(True) >= self.majority - 1:
            self.commit_index = self.last_log_index

        return pb2.ResponseAppendEntriesRPC(term=self.term, success=True)

    def follower_append(self, request):
        self.term = request.term
        self.current_leader = request.leaderId
        self.reset_timeout()
        print("here")
        if request.leaderCommit > self.commit_index:
            print("committing on follower")
            self.commit_index = request.leaderCommit
        # Remove unconsistent entries
        # if request.entry.command != "" and self.last_log_index > request.lastLogIndex:
        #     if self.log[request.lastLogIndex].term != request.term:
        #         del self.log[request.lastLogIndex:]
        print(f"follower index {self.last_log_index} and request index is {request.prevLogIndex}")
        print(f"follower log term {self.last_log_term} and request term is {request.prevLogTerm}")
        if (request.prevLogIndex == self.last_log_index + 1) and request.prevLogTerm >= self.last_log_term:
            print(f"received data for log index {self.last_log_index}")
            print(request)
            self.log[request.prevLogIndex] = request.entry
            self.last_log_index += 1
            self.last_log_term = request.prevLogTerm
            print('Follower got the message with new log')
            return pb2.ResponseAppendEntriesRPC(term=self.term, success=True)
        elif request.prevLogIndex == self.last_log_index and request.prevLogTerm == self.last_log_term:
            print('Follower got the message')
            print(f"got empty request {self.last_log_index}")
            return pb2.ResponseAppendEntriesRPC(term=self.term, success=True)
        else:
            print(f"Follower log_index is {self.last_log_index}")
            return pb2.ResponseAppendEntriesRPC(term=self.term, success=False)

    def AppendMessage(self, request, context):
        # if self.term < request.term:
        #     print("Term is lower than in request")
        #     return pb2.ResponseAppendEntriesRPC(term=self.term, success=False)
        if request.term > self.term:
            response = self.become_follower(request)
            return response

        if self.current_role == LEADER:
            response = self.leader_append(request)
            return response

        elif self.current_role == CANDIDATE:
            response = self.become_follower(request)
            return response

        elif self.current_role == FOLLOWER:
            response = self.follower_append(request)
            return response


if __name__ == '__main__':

    my_dict_address = {}
    with open('servers.txt', 'r') as f:
        line = f.readline()
        while line:
            temp_list = line.split()
            print(temp_list)
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
