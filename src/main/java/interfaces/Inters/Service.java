package main.java.interfaces.Inters;

import main.java.interfaces.DTOClient;
import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.nio.channels.ServerSocketChannel.open;
import static main.java.interfaces.constants.Constants.*;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 * @author nandhan, Created on 21/11/23
 */
public class Service{
    /*
        Zookeeper node is already been setup by the parent class.
        Zookeeper, latestCommitId, status, and data store should be shared with the parent.
        This class should extend Replica/Master class.
     */
    //private ExecutorService service;

    enum Status {
        LEADER,
        FOLLOWER
    }
    private String ipAddress;

    private Queue<DTO.KVPair>  pendingWriteRequests;
    private Queue<DTO.KVPair> finishedWriteRequests;

    private DTO.KVPair lastWriteRequest; // SEND THIS TO NEW LEADER.

    private Map<String,Object> dataStore;

    private ServerSocketChannel clientServer;
    //private SocketChannel masterSocket; // channel to communicate with leader.

    private Selector selector;

    //private int leaderPort;

    private Thread commitThread; // thread to participate in commit protocol with leader
    private Thread requestThread; // thread to send write requests to leader.

    private ZooKeeper zooKeeper;

    private volatile long latestCommitId;

    private volatile String currentLeaderAddress;
    private volatile boolean isLeader;
    private volatile boolean interruptThread;

    private Status status;

    //private String leaderPath;
    private String replicaId; // unique id of replica.
    private String zkReplicaPath; // stores absolute path to zookeeper node.
    //private String connectString;

    private Watcher leaderWatcher;

    private StringCallback leaderCreateCallback;

    Service(String zkConnect) {

        //this.zooKeeper = zooKeeper;
        //this.dataStore = dataStore;
        //this.latestCommitId = commitId;

        // establish watch on master node and get master address.
        //super(zkConnect);
        try {
            ipAddress = InetAddress.getLocalHost().getHostAddress(); //TODO ADD VALID ADDRESS.
            latestCommitId = INITIAL_COMMIT;
            zooKeeper = new ZooKeeper(zkConnect, ZK_TIMEOUT,null);
            System.out.println("Connected to Zookeeper");
            dataStore = new ConcurrentHashMap<>(INITIAL_TABLE_SIZE, LOAD_FACTOR, CONCURRENCY_LEVEL);
            replicaId = String.valueOf(new Random().nextLong(1,(long)1e5));
            isLeader = false;
            //currentLeaderAddress = null;
            lastWriteRequest = null;
            pendingWriteRequests = new ConcurrentLinkedQueue<>(); // fifo queue

            leaderWatcher = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    // check event type and take action
                    if (event.getType() == Event.EventType.NodeDeleted) {
                        System.out.println("Current Leader Crashed. Participating in election");
                        System.out.flush();
                        // participate in election
                        runForLeader(); // does it get the updated leader information?
                    }
                }
            };

            leaderCreateCallback = new StringCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, String name) {
                    // results from leader election
                    switch (KeeperException.Code.get(rc)) {
                        case OK -> {
                            /*
                            I'm the leader close connection with previous leader,
                            close connections with the clients'
                            setup Leader server for replicas.
                             */
                            System.out.println("I'm the leader.");

                            status = Status.LEADER;
                            isLeader = true;
                            interruptThread = true;

                        }
                        case CONNECTIONLOSS -> // try again
                                runForLeader();

                        case NODEEXISTS -> {
                            // leader already exists, get leader id
                            try {
                                System.out.println("Leader already exists");
                                // get new leader information.
                                var data = new String(zooKeeper.getData(LEADER_PATH, leaderWatcher, null));
                                System.out.println("current leader is: "+data);
                                System.out.flush();
                                if (!currentLeaderAddress.equals(data)) {
                                    // leader changed,
                                    // close current leader's connection and establish a new connection.
                                    // Interrupt the thread and
                                    interruptThread = true;
                                    //requestThread.interrupt();
                                    //if (masterSocket.isConnected())
                                    //    masterSocket.close(); // blocking operation?
                                    //Thread.sleep(5000); // TODO need this?
                                    currentLeaderAddress = data;
                                    //interruptThread = false;
                                    // start those two threads.
                                    //masterSocket.connect(new InetSocketAddress(currentLeaderAddress,leaderPort));

                                } // else old leader.

                            } catch (KeeperException | InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        default -> System.out.println("Something went wrong when running for leader");
                    }
                }
            };

        } catch (IOException e) {
            e.printStackTrace();
            // exit from program?
        }
    }

    void start() {
        try {
            // if leader node not exists, then participate in election.
            var stat = zooKeeper.exists(LEADER_PATH, leaderWatcher);
            if (stat  == null) {
                System.out.println("Leader not present");
                runForLeader();
            } else {
                currentLeaderAddress = new String(zooKeeper.getData(LEADER_PATH,leaderWatcher,stat));
                System.out.println("Current leader address is "+currentLeaderAddress);
            }


            Thread.sleep(SLEEP_TIME); // wait for callback

            //System.out.println("Value of isLeader is "+isLeader);
            if (!isLeader) {
                // setup zknode and server for clients
                setupZnode();
                setupClientServer();
                // start thread to send requests to leader and participate in election.
                interruptThread = false;
                startThreads();
                startAccepting();
            } else {
                System.out.println("Transitioning to leader mode. ");
                Leader leader = new Leader(this.zooKeeper,0,new ConcurrentLinkedQueue<>());
                leader.start();
            }

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //runForLeader();
    }

    void startThreads() {
        requestThread = new Thread(new ConnectionThread());
        requestThread.setPriority(Thread.MAX_PRIORITY-1);
        requestThread.start();
        // start the commit thread
        commitThread = new Thread(new CommitThread());
        commitThread.setPriority(Thread.MAX_PRIORITY);
        commitThread.start();
    }

    void runForLeaderBlocking() {
        try {
            var path = zooKeeper.create(LEADER_PATH, ipAddress.getBytes(),
                    OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            System.out.println("Im' the master");
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    void runForLeader() {
        // check if there is no leader
        System.out.println("Participating in election.");
        zooKeeper.create(LEADER_PATH,ipAddress.getBytes(),
                OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,leaderCreateCallback,null);

        //while(currentLeaderAddress != null)
            //Thread.onSpinWait();
    }

    void setupClientServer() {
        try {
            clientServer = open();
            clientServer.bind(new InetSocketAddress(CLIENT_PORT));
            clientServer.configureBlocking(false);

            selector = Selector.open();
            clientServer.register(selector, SelectionKey.OP_ACCEPT);
            System.out.println("Started server for Clients");

        } catch (IOException e) {
            e.printStackTrace();

            System.out.println("Unable to start server for clients: "+e.getMessage());
            System.exit(-1); // TODO check exit status
        }


    }

    void startAccepting() {
        // accept connections to clients and process them.
        while(!isLeader) {

            if (interruptThread) {
                // master is changed
                commitThread.interrupt();
                requestThread.interrupt();
                interruptThread = false;
                // start new threads.
                startThreads();
            }

            try {
                selector.select();

                var readyKeys = selector.selectedKeys().iterator();

                while (readyKeys.hasNext()) { // do not disturb this loop right after becoming leader?

                    var key = readyKeys.next();
                    readyKeys.remove();

                    if (key.isAcceptable()) {
                        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
                        SocketChannel client;// = serverSocketChannel.accept();
                        while((client = serverSocketChannel.accept()) == null) Thread.onSpinWait(); // TODO remove in future
                        System.out.println("Connected to client");
                        client.configureBlocking(false);
                        ClientAttachment attachment = new ClientAttachment(ByteBuffer.allocate(N_BYTES));
                        client.register(selector, CLIENT_OPS,attachment);

                    } else if (key.isReadable() && key.isWritable()) { // TODO CHECK LOGIC
                        // read the request and send a response.
                        SocketChannel client = (SocketChannel) key.channel();
                        ClientAttachment attachment = (ClientAttachment) key.attachment();
                        ByteBuffer buffer = attachment.buffer;
                        buffer.clear();
                        client.read(buffer);
                        buffer.flip();
                        DTOClient request = SerializationUtils.deserialize(buffer.array());
                        buffer.clear();
                        key.attach(attachment);

                        if (request.requestType == DTOClient.Type.GET) {
                            // get request, send response
                            var k = request.key;
                            if (dataStore.containsKey(k)) {
                                request.value = dataStore.get(k);
                                request.requestStatus = DTOClient.RequestStatus.OK;
                            } else
                                request.requestStatus = DTOClient.RequestStatus.NOT_FOUND;
                            //client.write(ByteBuffer.wrap(SerializationUtils.serialize(request)));
                        } else {
                            // put request
                            DTO.KVPair data = new DTO.KVPair(request.key,request.value);
                            // put in pending queue
                            pendingWriteRequests.add(data);
                            request.requestStatus = DTOClient.RequestStatus.OK;
                        }
                        // send response
                        client.write(ByteBuffer.wrap(SerializationUtils.serialize(request)));
                    }
                }

            } catch (IOException | SerializationException e) {
                e.printStackTrace();
            }
        }

        // If current process became leader in recent election.
        if (isLeader) {
            closeClientConnections();
            closeLeaderConnection();
            deleteNode();

            Queue<DTO.RequestObject> requestObjects = new ConcurrentLinkedQueue<>();
            pendingWriteRequests.forEach(request -> requestObjects.add(new DTO.RequestObject(replicaId,request)));

            Leader leader = new Leader(this.zooKeeper,latestCommitId, requestObjects);
            leader.start();


        }
        //closeClientConnections();
        //closeLeaderConnection();

        // TODO: delete this node in replicas.

        // new Leader().start();
    }



    void readyForLeader() {
        // close all client connections and pending requests.


    }

    void closeClientConnections() {

    }

    void closeLeaderConnection() {
        // stop the thread that
    }

    private void deleteNode() {
        try {
            zooKeeper.delete( zkReplicaPath, -1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    void setupZnode() {
        try {
            zkReplicaPath = zooKeeper.create(REPLICA_PATH+"/replica-",
                    ipAddress.getBytes(),OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL
            );
        } catch (KeeperException e) {

            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    void closeClientRequests() {
        // pass current requests to the leader.

    }

    class ConnectionThread implements Runnable {

        // TODO: REMOVE THIS METHOD AND FIELD.
        void connectToLeader() {
            /*
            try {
                masterSocket = SocketChannel.open(new InetSocketAddress(currentLeaderAddress,leaderPort));
            } catch (IOException e) {
                System.out.println("Unable to connect to Leader.");
            }

             */
        }

        @Override
        public void run() {
            //connectToLeader();
            // read queue and process them

            try (SocketChannel masterChannel = SocketChannel.open(
                    new InetSocketAddress(currentLeaderAddress,LEADER_PORT))
            ) {
                System.out.println("Connection Thread connected to Leader.");
                ByteBuffer buffer = ByteBuffer.allocate(32); // TODO CHANGE THIS

                // send the latest commit to leader.
                masterChannel.write(ByteBuffer.wrap(SerializationUtils.serialize(latestCommitId)));

                // read response from leader.
                masterChannel.read(buffer);
                buffer.flip();
                DTO.KVPair response = SerializationUtils.deserialize(buffer.array());
                buffer.clear();

                if (response.commitId > latestCommitId){
                    // TODO: vague? change in future.
                    dataStore.put(response.key, response.value);
                    latestCommitId = response.commitId;
                }

                // this replica is in sync with leader.

                while (!isLeader) {

                    if (interruptThread) {
                        // if interrupt thread variable becomes true then master
                        // would have changed, interrupt and stop this thread.
                        System.out.println("Interrupted by main thread.");
                        System.out.flush();
                        Thread.currentThread().interrupt();
                        break;
                    }

                    if (pendingWriteRequests.isEmpty()) {
                        Thread.onSpinWait();
                    }
                    else {
                        DTO.KVPair writeRequest = pendingWriteRequests.peek();

                        var object = new DTO.RequestObject(replicaId,writeRequest);
                        // write to channel.
                        masterChannel.write(ByteBuffer.wrap(SerializationUtils.serialize(object)));
                        System.out.println("Request: "+writeRequest.key + ", "+writeRequest.value + " sent to leader");

                        // wait for response. blocking.
                        masterChannel.read(buffer);
                        buffer.flip();
                        //DTO.COMMIT_RESPONSE response1 =  SerializationUtils.deserialize(buffer.array());

                        if (SerializationUtils.deserialize(buffer.array())
                                == DTO.COMMIT_RESPONSE.DONE){
                            // // write to normal queue.
                            // pop the request queue and
                            pendingWriteRequests.poll();
                        }
                        buffer.clear();
                    }
                }
                //buffer.clear();
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Exception occurred");
            }
            System.out.println("Thread exit");
        }
    }

    class CommitThread implements Runnable {
        /*
        This thread should responsible for participating in 2phase commit protocol.
        This thread should be blocked when group is in commit protocol.
         */
        //private SocketChannel commitChannel;
        private DTO.KVPair recentCommit;

        @Override
        public void run() {
            ByteBuffer buffer = ByteBuffer.allocate(N_BYTES);

            try (SocketChannel commitChannel = SocketChannel.open(
                    new InetSocketAddress(currentLeaderAddress, COMMIT_PORT))) {
                // PARTICIPATE IN PROTOCOL.

                System.out.println("Commit Thread connected to leader");
                while (!isLeader) {

                    if (interruptThread) {
                        System.out.println("Commit thread get interrupted");
                        System.out.flush();
                        Thread.currentThread().interrupt();
                        break;
                    }

                    try {
                        // receive commit request from leader
                        commitChannel.read(buffer);
                        buffer.flip();
                        DTO.KVPair request = SerializationUtils.deserialize(buffer.array());
                        buffer.clear();
                        //DTO.KVPair request = (DTO.KVPair) inputStream.readObject();
                        long commitNumber = request.commitId;

                        if (commitNumber > latestCommitId) {
                            // TODO: handle if the commit is old commit.
                            dataStore.put(request.key, request.value);
                            recentCommit = request;
                            latestCommitId = commitNumber;
                            var bytes = SerializationUtils.serialize(
                                    DTO.COMMIT_RESPONSE.ACKNOWLEDGEMENT);
                            commitChannel.write(ByteBuffer.wrap(bytes));
                            // todo: only process if commit is latest?
                            // store to local
                            dataStore.put(request.key, request.value);
                            latestCommitId = request.commitId;
                            //outputStream.writeObject(DTO.COMMIT_RESPONSE.ACKNOWLEDGEMENT);
                        } else {
                            var bytes = SerializationUtils.serialize(
                                    DTO.COMMIT_RESPONSE.OLD_COMMIT);
                            commitChannel.write(ByteBuffer.wrap(bytes));
                        }
                        // send ack to leader?
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    class ClientAttachment {
        ByteBuffer buffer; // non-direct buffer, holds backing array.

        ClientAttachment(int capacity) {
            this.buffer = ByteBuffer.allocate(capacity);
        }

        ClientAttachment (ByteBuffer buffer) {
            this.buffer = buffer;
        }
    }
}


