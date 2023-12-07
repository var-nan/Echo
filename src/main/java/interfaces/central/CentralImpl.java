package main.java.interfaces.central;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;

import static main.java.interfaces.constants.Constants.*;

/**
 * @author nandhan, Created on 24/11/23
 */
public class CentralImpl implements Central{

    private int INITIAL_VAL = 0;

    private ZooKeeper zooKeeper;

    private Map<String, Integer> assignment;

    //private DatagramSocket server;

    private Watcher leaderWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            // if current master is crashed, get new address.
        }
    };

    private Watcher replicaWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {

            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                // update list and
                updateList();
            }
        }
    };

    CentralImpl() {

        try {
            assignment = new HashMap<>();
            zooKeeper = new ZooKeeper("localhost", ZK_TIMEOUT, null);
            System.out.println("Connected to zookeeper");
            updateList();


        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void updateList() {

        //zooKeeper.getChildren()
        try {
            var replicas = zooKeeper.getChildren(REPLICA_PATH,replicaWatcher);
            //List<String> replicaAddresses = new ArrayList<>(replicas.size());
            for (String replica: replicas) {

                var address = new String(zooKeeper.getData(REPLICA_PATH+"/"+replica, false,null));
                if (!assignment.containsKey(address)) {
                    assignment.put(address,INITIAL_VAL);
                }
            }
            System.out.println("Replica List is updated.");
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void startListening() {

        // UDP connection

        try (DatagramSocket serverSocket = new DatagramSocket(CENTRAL_UDP_PORT)) {

            //var address = serverSocket.getLocalSocketAddress().toString();
            var address = serverSocket.getLocalAddress().getHostAddress();
            //var address = serverSocket.getRemoteSocketAddress().toString();
            System.out.println("Central address: " +address + " . Port: "+CENTRAL_UDP_PORT);

            while(true) {
                try {
                    DatagramPacket request = new DatagramPacket(
                            new byte[RCV_PACKET_SIZE], RCV_PACKET_SIZE
                    );
                    serverSocket.receive(request);

                    var serverData = getServer().getBytes();
                    DatagramPacket response = new DatagramPacket(serverData,
                            serverData.length, request.getAddress(), request.getPort()
                    );
                    serverSocket.send(response);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        } catch (SocketException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String getServer() {

        // get a server with the least amount of client connected to it.
        // update counter.

        String minServer = "";
        int minCount = Integer.MAX_VALUE;

        for (Map.Entry<String, Integer> entry: assignment.entrySet()) {
            if (entry.getValue() < minCount) {
                minCount = entry.getValue();
                minServer = entry.getKey();
            }
        }

        assignment.put(minServer, ++minCount);

        System.out.println("Returning  server "+minServer);
        return minServer;
    }

    public static void main(String[] args) {

        CentralImpl central = new CentralImpl();

        central.startListening();

    }
}
