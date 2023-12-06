package main.java.interfaces.Inters;

import org.apache.zookeeper.*;
import org.w3c.dom.Node;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;
import java.util.List;
import java.util.Map;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 * @author nandhan, Created on 02/11/23
 */
public class MasterReplica extends Master implements Replica {

    // TODO: create seperate threads to respond to client, other replicas and leader functionalities

    private ServerSocketChannel serverSocketChannel;

    private Map<String, Value> store;

    private NodeStatus nodeStatus;

    private ServerSocket socket;

    private static int TIMEOUT = 15000;
    private String path = "";

    //private boolean is_master = false;

    //private ZooKeeper zk;
    private static String hostPort;

    MasterReplica(String hostPort) throws IOException {
        super();
        this.hostPort = hostPort;
        if (status == Election.LEADER)
            this.nodeStatus = NodeStatus.LEADER;

        // accept for connections

        serverSocketChannel = ServerSocketChannel.open();

        // start zookeeper and connect to server

        //zk = new ZooKeeper(hostPort,TIMEOUT,);
        //isLeader = false;

        // create ephimeral node

        socket = new ServerSocket();
        //socket.bind();
    }

    private void createNode() throws InterruptedException, KeeperException {
        this.zk.create(path,new byte[1], OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    //@Override
    //public void process(WatchedEvent e) {
        // look for master node.

    //}




    public static void main(String[] args) throws IOException {

        MasterReplica replica = new MasterReplica("hello");
        replica.runForMaster();
        replica.takeLeadership();
        replica.cleanupDeletion();
        replica.processAddition();

        //replica.takeLeadership();
        //replica.broadcast(null,null);
    }

    @Override
    public void put(String key, Value value) {

    }

    @Override
    public Record get(String key, int version) {
        return null;
    }
}
