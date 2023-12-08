package main.java.interfaces.Inters;

import main.java.interfaces.DTOClient;
import org.apache.commons.lang3.SerializationUtils;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static main.java.interfaces.constants.Constants.*;

/**
 * @author nandhan, Created on 24/11/23
 */
public class ClientAPI {

    //private ZooKeeper zooKeeper;

    private String centralUrl; // TODO add this.

    private String serverUrl;

    private SocketChannel socketChannel;

    private ByteBuffer channelBuffer;

    //private DatagramSocket socket;

    private int port = CENTRAL_UDP_PORT;

    private final int replicaPort = CLIENT_PORT;

    public ClientAPI(String centralUrl) {
        this.centralUrl = centralUrl;
        channelBuffer = ByteBuffer.allocate(1024);
        // connect to centralUrl and get a server name
        //socket = new DatagramSocket();
        //connectToCentral();

        connectToServer();

    }

    private void connectToCentral() {
        //InetAddress centralAddress = null;

        System.out.println("Conneting to Central");

        try(DatagramSocket socket = new DatagramSocket()) {
            InetAddress centralAddress = InetAddress.getByName(centralUrl);
            byte[] buffer = new byte[256];// TODO CHANGE THIS.
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length,centralAddress,port);

            socket.send(packet);

            // get response
            packet = new DatagramPacket(buffer,buffer.length);
            socket.receive(packet);
            serverUrl = new String(packet.getData(),0,packet.getLength());

            System.out.println("Received Server Address from Central: " + serverUrl);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void connectToServer() {
        serverUrl = "10.26.187.200"; // TODO CHANGE THIS
        System.out.println("Connecting to server at "+serverUrl);
        System.out.flush();
        try {
            socketChannel = SocketChannel.open(new InetSocketAddress(serverUrl,replicaPort));
            System.out.println("Connected to server at "+serverUrl);
            //socket.bind(new InetSocketAddress(serverUrl, replicaPort));
            //DTOClient object = new DTOClient("home","India", DTOClient.Type.PUT);
            //var bytes = SerializationUtils.serialize(object);
            //socketChannel.write(ByteBuffer.wrap(bytes));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void Put(String key, Object value) {

        try {
            DTOClient object = new DTOClient(key, value, DTOClient.Type.PUT);
            var bytes = SerializationUtils.serialize(object);

            socketChannel.write(ByteBuffer.wrap(bytes));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Object Get(String key) {

        try {
            DTOClient object = new DTOClient(key, -1, DTOClient.Type.GET);

            // send request
            var bytes = SerializationUtils.serialize(object);
            socketChannel.write(ByteBuffer.wrap(bytes));

            // read response
            int nRead = socketChannel.read(channelBuffer);
            channelBuffer.flip();

            DTOClient response = SerializationUtils.deserialize(channelBuffer.slice(0,nRead).array());
            channelBuffer.clear();

            if (response.requestStatus == DTOClient.RequestStatus.OK)
                return response.value;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
