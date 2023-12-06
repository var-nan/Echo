package main.java.interfaces.Inters;

import java.nio.channels.ServerSocketChannel;

/**
 * @author nandhan, Created on 07/11/23
 */
public class ClientThread extends Thread{
    ServerSocketChannel channel;
    int port;

    ClientThread(ServerSocketChannel serverSocketChannel, int port) {
        this.channel = serverSocketChannel;
        this.port = port;
    }

    @Override
    public void run() {

    }

    // method to destroy all the connections to thread and not
}
