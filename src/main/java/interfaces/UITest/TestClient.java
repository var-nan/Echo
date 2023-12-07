package main.java.interfaces.UITest;

import main.java.interfaces.Inters.ClientAPI;

/**
 * @author nandhan, Created on 06/12/23
 */
public class TestClient {

    public static void main(String[] args) {

        ClientAPI client = new ClientAPI("localhost");

        client.Put("home","India");

        String value = (String)client.Get("home");

        System.out.println(value);

        assert value.equals("India");

    }
}
