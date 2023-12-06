package main.java.interfaces.Inters;


import org.apache.zookeeper.AsyncCallback.*;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;

// TODO: add logger to this class.
/**
 * @author nandhan, Created on 04/11/23
 */
public class Master{

    Election status;
    String serverPort = null;
    String leaderAddress = null;
    String serverAddress = "";
    boolean isConnected = false;

    long sessionId;

    boolean leaderElected = false;

    boolean isLeader = false;
    static final String replicaPath = "/replicas";
    static final String replicaPrefix = replicaPath + "/";
    static final String leaderPath = "/leader";
    String replicaName = ""; // TODO: name as ip address. or a long random number

    Watcher leaderWatcher;
    Watcher replicaWatcher;
    StatCallback leaderExistsCallback;
    StringCallback leaderCreateCallback;

    static final int TIMEOUT = 10000;


    ZooKeeper zk;

    List<String> activeReplicas;

    Master() {
        // constructor
        // initialize zookeeper

        this.status = Election.INIT;

        init();


        //Inet4Address.getLocalHost().getHostAddress();

        // check if master available

        leaderWatcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // leader crashed
                if (event.getType() == Event.EventType.NodeDeleted) {
                    assert event.getPath().equals(leaderPath);
                    runForLeader();
                }

                // do vote for leader.
            }
        };

        leaderExistsCallback = new StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS -> {
                        // do something
                    }
                    case OK -> {
                        // master is already running
                        try {
                            var data = zk.getData(leaderPath,leaderWatcher,null);
                            leaderAddress = new String(data);

                        } catch (KeeperException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        };

        leaderCreateCallback = new StringCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, String name) {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS ->  {
                        // check for master
                        isLeader = false;
                        runForLeader();
                    }
                    case OK -> {
                        isLeader = true;
                        takeLeadership();
                    }
                    case NODEEXISTS -> {
                        // leader already exists
                        isLeader = false;
                        leaderExists();
                        // set a watch on leader node
                    }
                    default -> {
                        isLeader = false;
                        // print something wrong when running for master.
                    }
                }
            }
        };

        replicaWatcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                switch (event.getType()) {
                    case NodeDeleted, NodeCreated -> {

                        // update activereplica list
                        // call getchildren and update list
                        try {
                            activeReplicas = zk.getChildren(replicaPath,replicaWatcher);
                            //activeReplicas = replicas;
                        } catch (KeeperException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                }
            }
        };

        this.status = Election.PARTICIPANT;
        runForLeader();

    }

    private void init() {

        // Establish connection with ZKServer and create znode
        try {
            if (!isConnected)
                zk = new ZooKeeper(serverPort,TIMEOUT,null); // TODO: check watcher
            isConnected = true;
            sessionId = zk.getSessionId();
            // create ephemeral node in workers
            zk.create(replicaPath+replicaName,null,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);


        } catch (IOException e) {
            // exit from this point
            // log about it
            System.exit(0); // TODO: need to add exit code
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException.NoChildrenForEphemeralsException e) {
            // no children for ephemeralnode
        } catch (KeeperException.NodeExistsException e) {
            System.out.println("Node exists");
            // thrown when try to create children to ephemeral node.
            e.printStackTrace();
        } catch (KeeperException e) {
            // do something
        }
    }


    private void runForLeader() {

        this.status = Election.PARTICIPANT;

        zk.create(leaderPath,replicaName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,leaderCreateCallback,null);

    }

    private void leaderExists() {
        zk.exists(leaderPath,leaderWatcher, leaderExistsCallback, null);
    }

    /*
    DataCallback  callback = null;
    ChildrenCallback childrenCallback  = new ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {

        }
    };

     */

    /*
    // this watcher is for replicas to watch on the master node.
    Watcher masterNodeWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if (!isLeader) {
                // create master node in server
                Stat stat;
                try {

                    stat = zk.exists(leaderPath,masterNodeWatcher);

                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    };

     */


    // this watcher for leader/master to watch on the replica node.
    /*
    Watcher replicaUpdateWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            // get child list and update the local list
            if (isLeader) {

                    zk.getChildren(replicaPath,this::process,childrenCallback,null);


            }
        }
    };


     */



    /**
     * implement leadership protocol
     */
    public void takeLeadership() {
        // when elected as leader, register for watch on replica path and remove watch on master
        zk.register(replicaWatcher);
    }


    public void process(WatchedEvent e) {
        // watch for master node
        // if worker nodes are updated, process updates.

        // if not leader, run for leader
        if (isLeader) {
            // if current process is leader, then register watch on /replicas path
            // if any replica is added/deleted? update the list.
            //try {
               // var list = zk.getChildren(replicaPath, true); // TODO: Need to add stat?

            //}
        }

    }
    
    
    
    //public void put(String key, Value value);

    //public Record get(String key, int version);

    /**
     * Broadcast the value to all active replicas
     * @param key
     * @param value
     */
    public void broadcast(String key, Value value) {
        if (isLeader) {

            // broadcast packet to all replicas via udp.

        }

    }

    /**
     * Returns list of active replicas.
     * @return
     */
    public List<String> getActiveReplicas() {

        List<String> list = null;

        try{
            list = this.zk.getChildren(replicaPath,true);
        } catch (KeeperException e) {

        } catch (InterruptedException e) {
            System.exit(0);
        }
        //this.zk.getChildren(replicaPath,true);
        list.get(0);

        return null;
    }


    public void cleanupDeletion() {
        //do something
    };
    
    public void processAddition() {
        // do something
    }
}
