package main.java.interfaces.Inters;

import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

import static main.java.interfaces.constants.Constants.ZK_TIMEOUT;

/**
 * @author nandhan, Created on 24/11/23
 */
public class Client {



    private ZooKeeper zooKeeper;

    private String url = "";

    Client() {

        try {

            // contact registry to get a replica.

            zooKeeper = new ZooKeeper(url, ZK_TIMEOUT, null);
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public void Put(String key, Object value) {

    }

    public Object Get(String key) {

        return null;

    }
}
