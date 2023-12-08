package main.java.interfaces.Inters;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import static main.java.interfaces.constants.Constants.*;

/**
 * @author nandhan, Created on 21/11/23
 */
public class Leader {

    private ZooKeeper zooKeeper;
    private long latestCommitId;
    private Map<String, Object> dataStore;

    private ServerSocketChannel replicaServer;
    private Selector replicaSelector;
    private int replicaCount;
    private Queue<DTO.RequestObject> requestQueue; // stores the list of write requests.
    private Queue<String> finishQueue; // stores list of replicaId whose requests are processed.

    private volatile List<String> replicas; // stores only address of replicas.

    private volatile boolean commitInProgress;

    private volatile int connectedReplicas;
    private volatile int activeReplicas;


    private final int replicaOps = SelectionKey.OP_READ | SelectionKey.OP_WRITE;

    private DTO.KVPair previousCommit;

    private final String replicaPath = "/replicas";

    private Watcher replicaWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            // only look for addition and deletion of replicas.
            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                // update replicas? delete current list and get new data.
                System.out.println("Replicas size changed.");
                updateReplicaCount();
            }
        }
    };

    private Thread commitThread;

    /**
     * Queries zookeeper to get list of replicas and
     * updates connectedReplicas variable.
     */
    void updateReplicaCount() {
        replicas.clear();
        try {
            var children = zooKeeper.getChildren(replicaPath, replicaWatcher);
            for (String child : children) {
                var data = zooKeeper.getData(replicaPath + "/" + child, false, null);
                replicas.add(new String(data));
            }
            connectedReplicas = replicas.size();
            // establish watch again?
            //zooKeeper.exists(replicaPath, re);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    Leader(ZooKeeper zooKeeper, Map<String, Object> dataStore, DTO.KVPair lastCommit, long latestCommitId, Queue<DTO.RequestObject> requestQueue) {
        //requestQueue = new ArrayDeque<>(); // TODO check again
        this.requestQueue = requestQueue;
        this.latestCommitId = latestCommitId;
        this.zooKeeper = zooKeeper;
        this.dataStore = dataStore;

        finishQueue = new ConcurrentLinkedQueue<>();

        previousCommit = lastCommit;
        commitInProgress = false;

        // TODO Initialize replicas collection
        replicas = new ArrayList<>(10);
        //connectedReplicas = 0;

        try {
            replicaServer = ServerSocketChannel.open();
            replicaServer.bind(new InetSocketAddress(LEADER_PORT));
            //replicaServer.configureBlocking(false);
            replicaSelector = Selector.open();
            //replicaServer.register(replicaSelector, LEADER_OPS);
            System.out.println("Server started for replicas");
            // set watch on replicas.
            //zooKeeper.getChildren(replicaPath,replicaWatcher);
            //zooKeeper.exists(replicaPath, replicaWatcher);
            updateReplicaCount();
            // TODO relinquish all streams.
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Starts accepting connections from replicas. Main Thread.
     * Stays in blocking mode until all replicas connect to it.
     *
     * If leader's latest commit is behind any replica's id. it recieves the latest commit from replica.
     * If any replica is behind leader's commit, then it receives the latest commit from leader.
     * @throws IOException
     */
    void start() {
        int count = 0;

        /*
        newly elected leader will get latest commitId from replicas.
        If leader is fallen behind, then leader will commit the txn.
        If replica is fallen behind, leader will send force commit to that replica.
         */
        while(connectedReplicas == 0) Thread.onSpinWait();

        System.out.println("Connected Replicas: "+connectedReplicas);
        try {
            // loop to accept replica connections
            while (count < connectedReplicas) {
                SocketChannel socket = replicaServer.accept();
                socket.configureBlocking(false);
                System.out.println("Connection Established: "+socket.socket().getInetAddress().getHostAddress());
                RepAttachment attachment = new RepAttachment(null,ByteBuffer.allocate(N_BYTES));
                socket.register(replicaSelector, REPLICA_OPS, attachment); // TODO add attachment
                count++;
            }

            this.replicaServer.configureBlocking(false);
            this.replicaServer.register(this.replicaSelector, LEADER_OPS);

            // get latest commit from all replicas
            count = 0;

            while (count < connectedReplicas) {

                replicaSelector.select();
                // select keys
                var readyKeys = replicaSelector.selectedKeys().iterator();

                while (readyKeys.hasNext()) {
                    SelectionKey key = readyKeys.next();
                    readyKeys.remove();

                    if (key.isReadable()) {
                        var replica = (SocketChannel) key.channel();
                        var attachment = (RepAttachment) key.attachment();

                        if (attachment.stage != null) {
                            // already got the latest request from this channel
                            key.attach(attachment);
                            continue;
                        }

                        var buffer = attachment.buffer;
                        replica.read(buffer);
                        buffer.flip();
                        DTO.KVPair replicaRecentCommit = SerializationUtils.deserialize(buffer.array());
                        buffer.clear();
                        attachment.stage = "READ_LATEST_COMMIT";
                        key.attach(attachment);

                        //CommitAttachment attachment = (CommitAttachment) key.attachment();

                        //ByteBuffer buffer = attachment.buffer;
                        //replica.read(buffer); // reads commitId.

                        //long recentCommit = Long.parseLong(StandardCharsets.UTF_8.decode(buffer).toString());

                        if (replicaRecentCommit.commitId > latestCommitId) {
                            // leader is behind one transaction
                            // commit this request and update local commit id.
                            var k = replicaRecentCommit.key;
                            var val = replicaRecentCommit.value;
                            dataStore.put(k, val);
                            latestCommitId = replicaRecentCommit.commitId;
                            previousCommit = replicaRecentCommit;
                            // break here? NO. READ buffers from other pending channels.
                        } else if (replicaRecentCommit.commitId < latestCommitId) {
                            // this replica is behind one transaction. Send the latest transaction to it.
                            //replica.write(ByteBuffer.wrap(SerializationUtils.serialize(previousCommit)));
                            //var serialize = attachment.outputStream;
                            //serialize.writeObject(replicaRecentCommit);
                            //serialize.flush();
                        }
                        count++;
                        //clearBuffer(buffer);
                    }
                }
            }

            // send the leader's latest commit to all replicas.
            count = 0;
            while(count < connectedReplicas) {

                replicaSelector.select();
                var readyKeys = replicaSelector.selectedKeys().iterator();

                while(readyKeys.hasNext()) {
                     SelectionKey key = readyKeys.next();
                     readyKeys.remove();

                     if (key.isWritable()) {
                         SocketChannel replica = (SocketChannel) key.channel();
                         RepAttachment attachment = (RepAttachment) key.attachment();

                         if (attachment.stage.equals("READ_LATEST_COMMIT")) {
                             // write to channel
                             replica.write(ByteBuffer.wrap(SerializationUtils.serialize(previousCommit)));
                             attachment.stage = "WRITE_LATEST_COMMIT";
                             count++;
                         }
                         key.attach(attachment); // attach object
                     }
                }
             }

            // All replicas are in sync with the leader. Can start servicing the requests with Commit Thread.
            commitThread = new Thread(new LeaderCommitThread());
            commitThread.setName("Commit Thread");
            commitThread.setPriority(Thread.MAX_PRIORITY);
            commitThread.start();


            startAccepting();

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    /**
     * Starts accepting write requests from replicas.
     * @throws IOException
     */
    private void startAccepting() throws IOException {

        // main thread

        while(true) {

            this.replicaSelector.select();

            var keys = this.replicaSelector.selectedKeys().iterator();

            while (keys.hasNext()) {
                var key = keys.next();
                keys.remove();

                if (key.isAcceptable()) {

                }

                // read new request from replica.
                if (key.isReadable()) {
                    SocketChannel replicaChannel = (SocketChannel) key.channel();

                    RepAttachment attachment = (RepAttachment)key.attachment();
                    var buffer = attachment.buffer;
                    replicaChannel.read(buffer); // TODO check how many bytes it will read.

                    buffer.flip();
                    DTO.RequestObject request =  SerializationUtils.deserialize(buffer.array());
                    buffer.clear();
                    attachment.id = request.replicaId;
                    request.KVPair.commitId = ++latestCommitId;
                    requestQueue.add(request);
                    key.attach(attachment);
                }
                else if (key.isWritable()) {

                    SocketChannel replicaChannel = (SocketChannel) key.channel();
                    RepAttachment attachment = (RepAttachment) key.attachment();

                    if (attachment.id != null) {
                        // check the finishQueue and send response.
                        // send the ack
                        var finished = finishQueue.peek();
                        if (attachment.id.equals(finished)) {
                            replicaChannel.write(
                                    ByteBuffer.wrap(SerializationUtils.serialize(DTO.COMMIT_RESPONSE.DONE))
                            );
                            finishQueue.poll();
                        }
                    }
                    else {
                        // it haven't made request yet.
                    }
                    key.attach(attachment);
                }
            }
        }

    }


    private void clearBuffer(ByteBuffer buffer) {
        buffer.clear();
    }

    enum CommitState {
        INIT,
        COMMIT,
    }

    class LeaderCommitThread implements Runnable {

        // this thread will processes write requests from the replicas.
        //

        private ServerSocketChannel commitServerChannel;
        private Selector commitReplicaSelector;

        private CommitState commitState;

        LeaderCommitThread() {
            // setup server socket
            try {
                this.commitServerChannel = ServerSocketChannel.open();
                this.commitServerChannel.bind(new InetSocketAddress(COMMIT_PORT)); // TODO change port number
                System.out.println("Leader Commit Thread Started");
                //this.commitServerChannel.configureBlocking(false);
                this.commitReplicaSelector = Selector.open();
                //this.commitServerChannel.register(clientSelector, SelectionKey.OP_ACCEPT);
                commitState = CommitState.INIT;

                // accept connections.
                int count = 0;
                while (count < connectedReplicas) {
                    // Accept connections from replicas.
                    SocketChannel sc = commitServerChannel.accept();
                    System.out.println("Connection Established");
                    sc.configureBlocking(false);

                    CommitAttachment attachment = new CommitAttachment(
                            INITIAL_COMMIT, // TODO change this
                            ByteBuffer.allocate(N_BYTES),
                            CommitState.INIT
                    );

                    sc.register(commitReplicaSelector, replicaOps, attachment);
                    count++;
                }

                // register server with the selector.
                this.commitServerChannel.configureBlocking(false);
                this.commitServerChannel.register(commitReplicaSelector, LEADER_OPS);

            } catch (IOException e) {
                e.printStackTrace();
            }
            // all replicas are connected.
            System.out.println("All replicas are connected.");
            System.out.flush();
        }

        void acceptConnection() throws IOException {
            this.commitReplicaSelector.select();

            var readyKeys = this.commitReplicaSelector.selectedKeys();
        }

        void closeConnections() {
            try {
                if (this.commitServerChannel.isOpen())
                    this.commitServerChannel.close();
                if (this.commitReplicaSelector.isOpen())
                    this.commitReplicaSelector.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        void commitRequest(DTO.RequestObject request) throws IOException {
            /*
            1. Leader sends a commit message to replicas.
            2. Leader receives ack, then commits to local db.
            3. Leader process next request.

            If leader fails during step 1:
               newly elected leader will get latest commitId from replicas.
               If leader is fallen behind, then leader will commit the txn.
               If replica is fallen behind, leader will send force commit to that replica.

           ** DO NOT ACCEPT NEW CONNECTIONS DURING THE PROTOCOL.
             */
            // TODO check commit Id in the request object, lower commitId causes the replica to ignore the commit. VERIFY ASAP
            commitInProgress = true;
            // PHASE 1: SEND COMMIT MESSAGE TO ALL REPLICAS.
            int count = 0;
            // TODO: IF A NEW CLIENT IS ADDED DURING PHASE 2?
            while (count < connectedReplicas) {

                commitReplicaSelector.select();
                var readyKeys = commitReplicaSelector.selectedKeys().iterator();

                while (readyKeys.hasNext()) {
                    var key = readyKeys.next();
                    readyKeys.remove();

                    if (key.isWritable()) {
                        SocketChannel sc = (SocketChannel) key.channel();
                        CommitAttachment attachment = (CommitAttachment) key.attachment();
                        /*
                        If attachment
                         */
                        if (attachment.state == CommitState.INIT  &&
                                attachment.commitId <= latestCommitId ) {
                            // todo: check the above boolean condition.
                            //ByteBuffer buffer = attachment.buffer;
                            // TODO write data to buffer.
                            //sc.write(buffer);
                            // TODO clear buffer

                            byte[] sendData = SerializationUtils.serialize(request.KVPair);
                            sc.write(ByteBuffer.wrap(sendData));
                            System.out.println("Sent Commit Request: "+(latestCommitId+1));
                            //attachment.outputStream.writeObject(request.KVPair);
                            //attachment.outputStream.flush();
                            attachment.commitId = latestCommitId + 1;
                            attachment.state = CommitState.COMMIT;
                            count++;
                        }
                        key.attach(attachment);
                    }
                }
            }

            System.out.println("Phase 1 completed.");

            // PHASE2: GET ACK FROM replicas.
            int currentCount = connectedReplicas;
            count = 0;
            // if replica fails during second phase of the protocol?.
            while (count < Math.min(currentCount,connectedReplicas)) {

                commitReplicaSelector.select();
                var readyKeys = commitReplicaSelector.selectedKeys().iterator();

                while (readyKeys.hasNext()) {
                    var key = readyKeys.next();
                    readyKeys.remove();

                    if (key.isReadable()) {
                        SocketChannel sc = (SocketChannel) key.channel();
                        CommitAttachment attachment = (CommitAttachment) key.attachment();

                        if (attachment.state == CommitState.COMMIT  // TODO CHECK THE CONDITION
                             && attachment.commitId == latestCommitId+1 ) {
                            // TODO change the commit id condition.
                            // read to buffer
                            //ByteBuffer buffer = attachment.buffer;
                            //sc.read(buffer);

                            // read "ACK" from buffer
                            //String response = "";
                            //if (response.equals("ACK")) {
                            //    attachment.state = CommitState.INIT;
                            //    count++;
                            //}
                            //clearBuffer(buffer);

                            sc.read(attachment.buffer);
                            attachment.buffer.flip();
                            var response = (DTO.COMMIT_RESPONSE) SerializationUtils.deserialize(attachment.buffer.array());
                            //clearBuffer(attachment.buffer);
                            attachment.buffer.clear();
                            //var response = (COMMIT_RESPONSE) attachment.inputStream.readObject();
                            if (response == DTO.COMMIT_RESPONSE.ACKNOWLEDGEMENT) {
                            // todo: handle this later.
                                System.out.println("Received Ack from server. Commit :"+(latestCommitId+1));
                            } else {

                            }
                            count++;
                        }
                        attachment.state = CommitState.INIT;
                        key.attach(attachment);
                    }
                    // accept new connection in phase 2?
                    else if (key.isAcceptable()) {
                        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
                        SocketChannel replica = serverSocketChannel.accept();

                        CommitAttachment newAttachment = new CommitAttachment(
                                INITIAL_COMMIT,
                                ByteBuffer.allocate(N_BYTES),
                                CommitState.INIT
                        );
                        replica.register(commitReplicaSelector, REPLICA_OPS, newAttachment);
                    }
                }
            }

            commitInProgress = false; // indicate that commit is not in progress.

            // commit current request to db, and increment commit id.
            dataStore.put(request.KVPair.key, request.KVPair.value);
            latestCommitId++;
            System.out.println("Commit "+latestCommitId + " completed.");
            System.out.flush();

        }

        @Override
        public void run() {
            /*
            Check request queue, if any pending requests, process them
            else sleep.
             */
            while (true) {
                if (requestQueue.isEmpty()) {

                    Thread.onSpinWait();
                    /*
                    try {
                        //Thread.onSpinWait();
                        //Thread.sleep(2000); // TODO replace this code with efficient instructions.

                    } catch (InterruptedException e) {
                        // relinquish all the resources
                        //closeConnections();
                    }

                     */
                } else {
                    // extract request.
                    var currentRequest = requestQueue.poll();

                    if (currentRequest == null)
                        continue;
                    // send a commit prepare message to replica threads.
                    try {
                        commitRequest(currentRequest);
                        previousCommit = currentRequest.KVPair;
                    } catch (IOException e) {
                        System.out.println("Exception Occurred");
                    }
                }
            }
        }
    }

    class RepAttachment {
        ObjectOutputStream outputStream;
        ObjectInputStream inputStream;
        ByteBuffer buffer;

        String stage;
        String id;

        RepAttachment(String id, ByteBuffer buffer) {
            this.id = id;
            this.buffer = buffer;
        }
    }

    class CommitAttachment {

        ByteBuffer buffer;
        CommitState state;
        long commitId;
        //ObjectOutputStream outputStream;
        //ObjectInputStream inputStream;
        //byte[] byteArray;

        CommitAttachment(long commitId, ByteBuffer buffer, CommitState state) {
            this.commitId = commitId;
            this.buffer = buffer;
            this.state = state;
        }
    }
}