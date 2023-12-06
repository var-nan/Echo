package main.java.interfaces.Inters;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * @author nandhan, Created on 24/11/23
 */
public class Application {

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        String zookeeperString = "localhost";
        Service service = new Service(zookeeperString);

        service.start();

        //service.setupClientServer();

        //service.startAccepting();

    }
}
