package main.java.interfaces.utils;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

import static main.java.interfaces.constants.Constants.*;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 * @author nandhan, Created on 05/11/23
 */
public class SetupScript {

    private ZooKeeper zooKeeper;

    SetupScript(){
        try {
            this.zooKeeper = new ZooKeeper("localhost",ZK_TIMEOUT, null);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setupZK() {

        try {
            // delete leader if exists?
            //Stat stat = zooKeeper.exists(LEADER_PATH,false);

            //zooKeeper.delete(LEADER_PATH, -1);
            zooKeeper.create(REPLICA_PATH,null, OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public void cleanZK() {
        try {
            zooKeeper.delete(REPLICA_PATH,-1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        System.out.println("Setting up zookeeper");
        SetupScript script = new SetupScript();
        script.setupZK();
        System.out.println("Zookeeper setup complete");
    }
}
