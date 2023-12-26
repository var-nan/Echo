# Echo

## A Sequentially Consistent Replicated Key-Value data store.


## API

1. **Get(key)**: This method retrieves the value corresponding to the given key from the currently connected replica.
   The system is expected to provide stale read values until the latest commit is received by the connected replica.
3. **Put(key,value)**: This method puts the value with the given key in the data store. In the current instance, a replica can send at most one write request in its request queue.
   
## Architecture

#### Leader
The leader orders every write request in the data store. When a leader receives a write request from the replica, 
it assigns a unique number to it and performs its commit. Since all the requests are added to the queue, there exists a valid sequence to order the requests. 
The leader uses two threads to commit a transaction (one to listen to write requests from the replicas and one to start the commit protocol).

![Threads](https://github.com/var-nan/Echo/blob/master/architecture2.png "Threads")

#### Replica

The replica receives requests from the clients. If the request is of type 'get', then the replica will serve the request from its local datastore. 
If the request is of type 'put', it sends the request to the leader to commit the write to all the other replicas in the same order. 
The replica is a three-threaded program that maintains a server to handle connections from clients and a socket to connect to the leader.

![system architecture](https://github.com/var-nan/Echo/blob/master/architecture.png "System architecture")

## Dependencies

1. Java SE 17
2. Gradle
3. Apache ZooKeeper
4. Apache Commons Lang
5. Log4J

Gradle automatically installs the above dependencies during the project build.

Download the [Apache ZooKeeper](https://dlcdn.apache.org/zookeeper/zookeeper-3.8.3/apache-zookeeper-3.8.3-bin.tar.gz) to setup ZooKeeper server.

## Installation steps


1. Download the latest repository from git.
2. Open the project in any (IntelliJ preferred) IDE and build the project.
3. Start the ZooKeeper Server on port 8000.
4. Run SetupScript.java to set up znodes in ZooKeeper Server.

5. Run Central.java to start the load balancer.
6. Run Service.java on a separate machine.
7. start multiple instances of Service.java (except steps 3 and 4) in different machines to set the desired number of replicas.
8. Run Client.java on a different machine (multiple instances for multiple clients)

Replicas and clients can be run in a single machine in a standalone environment.

## Contribution

We welcome any feedback and suggestions to this project.
