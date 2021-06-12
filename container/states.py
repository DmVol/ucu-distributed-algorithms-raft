import random
from queue import Queue
import time
import proto.raft_pb2 as pb2
import threading
import logging


FOLLOWER = "follower"
CANDIDATE = "candidate"
LEADER = "leader"


class State:

    server: "Server"

    def reset_timeout(self):
        self.server.timeout = time.time() + random.randint(5, 10)

    def init_timeout(self):
        self.reset_timeout()
        # safety guarantee, timeout thread may expire after election
        if self.server.timeout_thread and self.server.timeout_thread.isAlive():
            return
        self.server.timeout_thread = threading.Thread(target=self.timeout_loop)
        self.server.timeout_thread.start()

    def timeout_loop(self):
        # only stop timeout thread when winning the election
        while self.server.current_role != LEADER:
            delta = self.server.timeout - time.time()
            if delta < 0:
                self.server.current_role = CANDIDATE
            else:
                time.sleep(delta)

    def vote(self, request, context):
        term_ok, log_ok = False, False

        # Check log
        if request.lastLogTerm > self.server.last_log_term:
            log_ok = True
        elif request.lastLogTerm == self.server.last_log_term and request.lastLogIndex >= self.server.last_log_index:
            log_ok = True
        else:
            log_ok = False

        # Check term
        if request.term > self.server.term:
            term_ok = True
        elif request.term == self.server.term and self.server.voted_for in (None, request.candidateId):
            term_ok = True
        else:
            term_ok = False

        # Return response
        if term_ok and log_ok:
            self.server.term = request.term
            self.server.current_role = FOLLOWER
            self.server.voted_for = request.candidateId
            self.reset_timeout()
            return pb2.ResponseVoteRPC(term=self.server.term, voteGranted=True)
        else:
            return pb2.ResponseVoteRPC(term=self.server.term, voteGranted=False)

    def list_messages(self, request):
        print(f"Node {self.server.id} commit index is {self.server.commit_index}")
        response = pb2.ListMessagesResponse(logs=list(self.server.log.values()))
        return response


class Leader(State):
    server: "Server"

    def __init__(self, server: "Server"):
        self.server = server
        self.server.current_role = LEADER

    def broadcast(self, node_id, prevLogIndex, request, stub):
        if prevLogIndex in self.server.log:
            entry = self.server.log[prevLogIndex]
        else:
            entry = None
        try:
            response = stub.AppendMessage(request)
            while response.success == False:
                if prevLogIndex >= 1:
                    prevLogIndex -= 1
                logging.info(f"sending entry with index -  index no {prevLogIndex}, current index is {self.server.last_log_index}")
                entry = self.server.log[prevLogIndex]
                request = pb2.RequestAppendEntriesRPC(term=self.server.term, leaderId=self.server.id,
                                                      prevLogIndex=prevLogIndex, prevLogTerm=entry.term,
                                                      entry=entry, leaderCommit=self.server.commit_index)
                response = stub.AppendMessage(request)

            while prevLogIndex < self.server.last_log_index:
                prevLogIndex += 1
                logging.info(f"sending entry with index -  index no {prevLogIndex}, current index is {self.server.last_log_index}")
                entry = self.server.log[prevLogIndex]
                request = pb2.RequestAppendEntriesRPC(term=self.server.term, leaderId=self.server.id,
                                                      prevLogIndex=prevLogIndex, prevLogTerm=entry.term,
                                                      entry=entry, leaderCommit=self.server.commit_index)
                response = stub.AppendMessage(request)

            return response

        except:
            print('cannot connect to ' + str(self.server.port_addr[node_id]))

    def run(self):
        logging.info(f"{self.server.id} is leader")
        if time.time() > self.server.timeout:
            logging.info(f"Node {self.server.id} start sending requests")
            prevLogIndex = self.server.last_log_index
            if prevLogIndex in self.server.log:
                entry = self.server.log[prevLogIndex]
            else:
                entry = None
            # Append Entries Request.
            logging.info(f"my term is {self.server.term} last log term is {self.server.last_log_term}")
            request = pb2.RequestAppendEntriesRPC(term=self.server.term, leaderId=self.server.id, prevLogIndex=prevLogIndex,
                                                  prevLogTerm=self.server.last_log_term, entry=entry,
                                                  leaderCommit=self.server.commit_index)

            for node_id, stub in enumerate(self.server.stub_list):
                threading.Thread(target=self.broadcast,
                                 args=(node_id, prevLogIndex, request, stub)).start()

            self.server.timeout = time.time() + 1

    def append(self, request):
        if request.term > self.server.term:
            response = self.server.become_follower(request)
            return response
        entry = pb2.LogEntry(term=self.server.term, command=request.entry.command)
        self.server.last_log_term = self.server.term
        self.server.last_log_index += 1
        self.server.log[self.server.last_log_index] = entry

        req = pb2.RequestAppendEntriesRPC(term=self.server.term, leaderId=self.server.id, prevLogIndex=self.server.last_log_index,
                                          prevLogTerm=self.server.last_log_term, entry=entry,
                                          leaderCommit=self.server.commit_index)

        # Create objects for threads result handling
        que = Queue()
        thread_list = []
        responses = []

        for node_id, stub in enumerate(self.server.stub_list):
            t = threading.Thread(target=lambda q, arg1, arg2, arg3, arg4: q.put(self.broadcast(arg1, arg2, arg3, arg4)),
                                 args=(que, node_id, self.server.last_log_index, req, stub))
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
        if responses.count(True) >= self.server.majority - 1:
            self.server.commit_index = self.server.last_log_index

        return pb2.ResponseAppendEntriesRPC(term=self.server.term, success=True)

    def become_follower(self, request):
        logging.info(f"term in request is greater than mine node {self.server.id} is becoming follower")
        self.server.term = request.term
        self.server.voted_for = -1
        # self.server.current_role = FOLLOWER
        self.server.current_leader = request.leaderId
        self.reset_timeout()
        self.server.become(Follower)
        return pb2.ResponseAppendEntriesRPC(term=self.server.term, success=True)


class Candidate(State):
    server: "Server"

    def __init__(self, server: "Server"):
        self.server = server
        self.server.current_role = CANDIDATE

    def ask_vote(self, request, stub):
        try:
            response = stub.Vote(request)
            if response.voteGranted:
                self.server.votes_received += 1

        except:
            print("connection error")

    def run(self):
        if time.time() > self.server.timeout:
            self.server.term += 1
            self.server.votes_received = 1
            logging.info(f"node {self.server.id} is becoming candidate, start sending vote requests")
            request = pb2.RequestVoteRPC(term=self.server.term, candidateId=self.server.id, lastLogIndex=self.server.last_log_index,
                                         lastLogTerm=self.server.last_log_term)

            barrier = threading.Barrier(self.server.majority - 1, timeout=2)
            for stub in self.server.stub_list:
                threading.Thread(target=self.ask_vote,
                                 args=(request, stub)).start()

            barrier.wait()
            # print(self.server.votes_received)
            self.reset_timeout()
        elif self.server.votes_received >= self.server.majority:
            logging.info(f"node {self.server.id} received majority votes, becoming leader")
            # self.server.current_role = LEADER
            self.server.votes_received = 1
            self.server.voted_for = self.server.id
            self.server.timeout = time.time()
            self.server.become(Leader)

    def become_follower(self, request):
        logging.info(f"term in request is greater than mine node {self.server.id} is becoming follower")
        self.server.term = request.term
        self.server.voted_for = -1
        # self.server.current_role = FOLLOWER
        self.server.current_leader = request.leaderId
        self.reset_timeout()
        self.server.become(Follower)
        return pb2.ResponseAppendEntriesRPC(term=self.server.term, success=False)

    def append(self, request):
        if request.term > self.server.term:
            response = self.server.become_follower(request)
            return response


class Follower(State):
    server: "Server"

    def __init__(self, server: "Server"):
        self.server = server
        self.server.current_role = FOLLOWER

    def run(self):
        if time.time() > self.server.timeout:
            logging.info(f"timeout reached for node {self.server.id}, becoming candidate")
            self.server.become(Candidate)

    def append(self, request):
        self.server.term = request.term
        self.server.current_leader = request.leaderId
        self.reset_timeout()
        if request.leaderCommit > self.server.commit_index:
            logging.info(f"follower node {self.server.id} commit sync")
            self.server.commit_index = request.leaderCommit
        # Remove unconsistent entries
        # if request.entry.command != "" and self.server.last_log_index > request.lastLogIndex:
        #     if self.server.log[request.lastLogIndex].term != request.term:
        #         del self.server.log[request.lastLogIndex:]
        logging.info(f"follower index {self.server.last_log_index} and request index is {request.prevLogIndex}")
        logging.info(f"follower log term {self.server.last_log_term} and request term is {request.prevLogTerm}")
        logging.info(f"follower term {self.server.term}")
        if (request.prevLogIndex == self.server.last_log_index + 1) and request.prevLogTerm >= self.server.last_log_term:
            logging.info(f"received data for log index {self.server.last_log_index}")
            self.server.log[request.prevLogIndex] = request.entry
            self.server.last_log_index += 1
            self.server.last_log_term = request.prevLogTerm
            logging.info(f"follower {self.server.id} received the message with new log")
            return pb2.ResponseAppendEntriesRPC(term=self.server.term, success=True)
        elif request.prevLogIndex == self.server.last_log_index and request.prevLogTerm == self.server.last_log_term:
            logging.info(f"follower {self.server.id} received heartbeat")
            return pb2.ResponseAppendEntriesRPC(term=self.server.term, success=True)
        else:
            logging.info(f"follower {self.server.id} aborted request")
            return pb2.ResponseAppendEntriesRPC(term=self.server.term, success=False)