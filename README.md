# ucu-distributed-algorithms-raft
Raft implementation using python and grpc

# Cluster structure
The cluster consists of 5 nodes and a network, the docker-compose command initialize every node as a docker container
The configs for every node are located in server.txt file (this file contains node id and internal address)
For adding node, it will be needed to modify server.txt file and docker-compose command

# Setting up

docker-compose up -d --build

# Commands for network partitioning testing

--Create new network

docker network create test-1

--Exclude node from original network and connect to a new one

docker network disconnect ucu-distributed-algorithms-raft_localnet raft-node-1

docker network connect test-1 raft-node-1

--Disconnect from test network and connect to original network

docker network disconnect test-1 raft-node-1

docker network connect ucu-distributed-algorithms-raft_localnet raft-node-1
