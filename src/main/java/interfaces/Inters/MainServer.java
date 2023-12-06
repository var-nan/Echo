package main.java.interfaces.Inters;

import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import static main.java.interfaces.constants.Constants.*;

/**
 * @author nandhan, Created on 21/11/23
 */
public class MainServer {

    //static final int TIMEOUT = 20000;

    static ZooKeeper zooKeeper;
    static volatile long latestCommitId;
    static Map<String, Object> dataStore;
    static volatile boolean isLeader;
    static String replicaId;
    static String connectString;
    static Status status;
    static String leaderPath;

    MainServer(String str) throws IOException {
        latestCommitId = INITIAL_COMMIT;
        connectString = str;
        zooKeeper = new ZooKeeper(connectString, ZK_TIMEOUT,null);
        dataStore = new ConcurrentHashMap<>();
        replicaId = String.valueOf(new Random().nextLong(1,(long)1e5));
        isLeader = false;
        leaderPath = LEADER_PATH;
    }


    enum Status {
        LEADER,
        FOLLOWER
    }
}
