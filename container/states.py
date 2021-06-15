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
        self.server.timeout = time.time() + random.randint(120, 160)

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
            self.reset_timeout()
            self.server.term = request.term
            self.server.become(Follower)
            self.server.voted_for = request.candidateId
            return pb2.ResponseVoteRPC(term=self.server.term, voteGranted=True)
        else:
            return pb2.ResponseVoteRPC(term=self.server.term, voteGranted=False)

    def list_messages(self, request):
        logging.info(f"Node {self.server.id} commit index is {self.server.commit_index}")
        print(f""" ************ Node {self.server.id} current state is  ***********
                                commit index = {self.server.commit_index}
                                last log index = {self.server.last_log_index}
                                term = {self.server.term}
                                last log term = {self.server.last_log_term}
                                current leader = {self.server.current_leader}
                    *************************************************""")
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

            response = stub.AppendMessage(request, timeout=1)

            if response.success == False and response.term > self.server.term:
                logging.info(f"killing thread on node {self.server.id}")
                self.reset_timeout()

            time.sleep(random.uniform(0, 0.5))

            while response.success == False:
                if prevLogIndex >= 1:
                    prevLogIndex -= 1
                logging.info(
                    f"sending entry with index -  index no {prevLogIndex}, current index is {self.server.last_log_index}")
                entry = self.server.log[prevLogIndex]
                request = pb2.RequestAppendEntriesRPC(term=self.server.term, leaderId=self.server.id,
                                                      prevLogIndex=prevLogIndex, prevLogTerm=entry.term,
                                                      entry=entry, leaderCommit=self.server.commit_index)
                response = stub.AppendMessage(request, timeout=1)

            while prevLogIndex < self.server.last_log_index:
                prevLogIndex += 1
                logging.info(
                    f"sending entry with index -  index no {prevLogIndex}, current index is {self.server.last_log_index}")
                entry = self.server.log[prevLogIndex]
                request = pb2.RequestAppendEntriesRPC(term=self.server.term, leaderId=self.server.id,
                                                      prevLogIndex=prevLogIndex, prevLogTerm=entry.term,
                                                      entry=entry, leaderCommit=self.server.commit_index)
                response = stub.AppendMessage(request, timeout=1)

            return response

        except:

            logging.info('cannot connect to ' + str(self.server.port_addr[node_id]))

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
            request = pb2.RequestAppendEntriesRPC(term=self.server.term, leaderId=self.server.id,
                                                  prevLogIndex=prevLogIndex, prevLogTerm=self.server.last_log_term,
                                                  entry=entry, leaderCommit=self.server.commit_index)

            threads = []

            for node_id, stub in enumerate(self.server.stub_list):
                t = threading.Thread(target=self.broadcast,
                                     args=(node_id, prevLogIndex, request, stub))
                threads.append(t)
                t.start()

            for x in threads:
                x.join()

            self.server.timeout = time.time() + 2

    def append(self, request):
        if request.term > self.server.term:
            response = self.become_follower(request)
            return response

        if request.term < self.server.term:
            logging.info(f"omg, you are pinging leader, how dare you??!!")
            return pb2.ResponseAppendEntriesRPC(term=self.server.term, success=False)

        entry = pb2.LogEntry(term=self.server.term, command=request.entry.command)
        self.server.last_log_term = self.server.term
        self.server.last_log_index += 1
        self.server.log[self.server.last_log_index] = entry

        req = pb2.RequestAppendEntriesRPC(term=self.server.term, leaderId=self.server.id,
                                          prevLogIndex=self.server.last_log_index,
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
            # print(responses, responses.count(True))

        # Count all approves
        if responses.count(True) >= self.server.majority - 1:
            self.server.commit_index = self.server.last_log_index

        return pb2.ResponseAppendEntriesRPC(term=self.server.term, success=True)

    def become_follower(self, request):
        logging.info(f"term in request is greater than mine node {self.server.id} is becoming follower")
        self.server.term = request.term
        self.server.voted_for = -1
        self.server.current_leader = request.leaderId
        self.reset_timeout()
        self.server.become(Follower)
        return pb2.ResponseAppendEntriesRPC(term=self.server.term, success=True)


class Candidate(State):
    server: "Server"

    def __init__(self, server: "Server"):
        self.server = server
        self.server.current_role = CANDIDATE

    def ask_vote(self, barrier, request, stub):
        try:
            response = stub.Vote(request, timeout=1)
            if response.voteGranted:
                self.server.votes_received += 1
                logging.info(f"node {self.server.id} received {self.server.votes_received} votes")
                barrier.wait()

        except:
            logging.info("connection error")

    def run(self):
        if time.time() > self.server.timeout:
            self.server.term += 1
            self.server.votes_received = 1
            logging.info(f"majority is equal to {self.server.majority}")
            logging.info(f"node {self.server.id} is becoming candidate, start sending vote requests")
            request = pb2.RequestVoteRPC(term=self.server.term, candidateId=self.server.id,
                                         lastLogIndex=self.server.last_log_index,
                                         lastLogTerm=self.server.last_log_term)

            barrier = threading.Barrier(self.server.majority - 1, timeout=1)

            for stub in self.server.stub_list:
                threading.Thread(target=self.ask_vote,
                                 args=(barrier, request, stub)).start()

            # barrier.wait()
            logging.info(str(barrier.broken) + "\n")
            barrier.reset()
            logging.info("n_waiting after reset = " + str(barrier.n_waiting))
            barrier.abort()
            logging.info("End")
            logging.info(f"node {self.server.id} received {self.server.votes_received} votes")
            self.reset_timeout()
        elif self.server.votes_received >= self.server.majority:
            logging.info(f"node {self.server.id} received majority votes, becoming leader")
            self.server.votes_received = 1
            self.server.voted_for = self.server.id
            self.server.timeout = time.time()
            self.server.become(Leader)

    def become_follower(self, request):
        logging.info(f"term in request is greater than mine node {self.server.id} is becoming follower")
        self.server.term = request.term
        self.server.voted_for = -1
        self.server.current_leader = request.leaderId
        self.reset_timeout()
        self.server.become(Follower)
        return pb2.ResponseAppendEntriesRPC(term=self.server.term, success=False)

    def append(self, request):
        if request.term > self.server.term:
            response = self.become_follower(request)
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

        if request.term < self.server.term:
            logging.info(f"follower term :{self.server.term} is greater that in request {request.term}")
            return pb2.ResponseAppendEntriesRPC(term=self.server.term, success=False)

        # Remove uncommitted entries
        if self.server.last_log_term < request.prevLogTerm and self.server.last_log_index > self.server.commit_index:
            logging.info(f"follower node {self.server.id} removing entity {request.prevLogIndex}")
            self.server.log.pop(request.prevLogIndex, None)
            self.server.last_log_index = self.server.commit_index
            return pb2.ResponseAppendEntriesRPC(term=self.server.term, success=False)

        if request.leaderCommit > self.server.commit_index:
            logging.info(f"follower node {self.server.id} commit sync")
            for i in range(self.server.commit_index, request.leaderCommit + 1):
                if i in self.server.log and request.prevLogTerm == self.server.last_log_term:
                    self.server.commit_index = i

        logging.info(f"follower index {self.server.last_log_index} and request index is {request.prevLogIndex}")
        logging.info(f"follower log term {self.server.last_log_term} and request term is {request.prevLogTerm}")
        logging.info(f"follower term {self.server.term}")

        if (request.prevLogIndex == self.server.last_log_index + 1) and request.prevLogTerm >= self.server.last_log_term:
            logging.info(f"received data for log index {self.server.last_log_index}")
            self.server.log[request.prevLogIndex] = request.entry
            self.server.last_log_index += 1
            self.server.last_log_term = request.prevLogTerm
            logging.info(f"follower {self.server.id} received the message with new log {request.prevLogIndex}")
            return pb2.ResponseAppendEntriesRPC(term=self.server.term, success=True)

        elif request.prevLogIndex == self.server.last_log_index and request.prevLogTerm == self.server.last_log_term:
            logging.info(f"follower {self.server.id} received heartbeat")
            return pb2.ResponseAppendEntriesRPC(term=self.server.term, success=True)

        else:
            logging.info(f"follower {self.server.id} aborted request")
            return pb2.ResponseAppendEntriesRPC(term=self.server.term, success=False)
