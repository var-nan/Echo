package main.java.interfaces.constants;

import java.nio.channels.SelectionKey;

/**
 * @author nandhan, Created on 23/11/23
 */
public class Constants {

    public static final int COMMIT_PORT = 5002; // leader opens this port for committing.
    public static final int LEADER_PORT = 5000; // leader opens this port and replicas connect to this port for sending write requests..
    public static final int CLIENT_PORT = 5005;

    public static final int N_BYTES = 1024;

    public static final int REPLICA_OPS = SelectionKey.OP_READ | SelectionKey.OP_WRITE;
    public static final int CLIENT_OPS = SelectionKey.OP_READ | SelectionKey.OP_WRITE;
    public static final int LEADER_OPS = SelectionKey.OP_ACCEPT;

    public static final int INITIAL_COMMIT = 1;

    //static final String ACK = "ACK";
    //static final String OLD_COMMIT = "OLD_COMMIT";

    public static final int ZK_TIMEOUT = 20000;
    public static final String LEADER_PATH = "/leader";
    public static final String REPLICA_PATH = "/replicas";

    public static final int INITIAL_REPLICAS = 3; // minimum number of replicas needed to start the service.

    public static final int SEND_PACKET_SIZE = 64;
    public static final int RCV_PACKET_SIZE = 64;
    public static final int CENTRAL_UDP_PORT = 6000;

    public static final int SLEEP_TIME = 5000;

    public static final int INITIAL_TABLE_SIZE = 64;
    public static final int CONCURRENCY_LEVEL = 3;
    public static final float LOAD_FACTOR = (float) 0.9;
}
