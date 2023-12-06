package main.java.interfaces.central;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static main.java.interfaces.constants.Constants.*;

/**
 * @author nandhan, Created on 24/11/23
 */
public class Central {

    private int INITIAL_VAL = 0;

    private ZooKeeper zooKeeper;

    private Map<String, Integer> assignment;

    //private DatagramSocket server;

    private Watcher leaderWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            // get master address
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

    Central() {

        try {
            assignment = new HashMap<>();
            zooKeeper = new ZooKeeper("rurl", ZK_TIMEOUT, null);


        } catch (IOException e) {

        }
    }

    private void updateList() {

        //zooKeeper.getChildren()
        try {
            var replicas = zooKeeper.getChildren("/replicas",replicaWatcher);
            //List<String> replicaAddresses = new ArrayList<>(replicas.size());
            for (String replica: replicas) {
                var address = new String(zooKeeper.getData(replica, false,null));
                if (!assignment.containsKey(address)) {
                    assignment.put(address,INITIAL_VAL);
                }
            }

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void startListening() {

        try (DatagramSocket serverSocket = new DatagramSocket(CENTRAL_UDP_PORT)) {

            while(true) {
                try {
                    DatagramPacket request = new DatagramPacket(
                            new byte[RCV_PACKET_SIZE], RCV_PACKET_SIZE
                    );
                    serverSocket.receive(request);
                    var serverData = SerializationUtils.serialize(getServer());
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
        return minServer;
    }

    public static void main(String[] args) {

        Central central = new Central();

        central.startListening();

    }
}
