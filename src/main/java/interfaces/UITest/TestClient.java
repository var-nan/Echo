package main.java.interfaces.UITest;

import main.java.interfaces.Inters.ClientAPI;

import java.util.Scanner;

/**
 * @author nandhan, Created on 06/12/23
 */
public class TestClient {

    public static void main(String[] args) {
        ClientAPI client = new ClientAPI("localhost");

        /*
        client.Put("home","India");
        client.Put("city","Ongole");

        String value = (String)client.Get("home");
        String state = (String)client.Get("city");


        System.out.println(value);
        System.out.println(state);
        assert value.equals("India");
        assert state.equals("Ongole");

         */

        cli(client);

    }

    public static void cli(ClientAPI client) {

        try (Scanner sc = new Scanner(System.in)) {

            System.out.println("Enter request type: 1. Put \t 2. Get \t 0. Quit");
            int choice;

            while((choice = sc.nextInt()) != 0) {
                sc.nextLine();

                if (choice == 1) {
                    System.out.println("Enter key: ");
                    String key = sc.next();
                    sc.nextLine();
                    System.out.println("Enter value");
                    String value = sc.next();
                    System.out.println("Entered values: "+ key + ". \t value: "+value);
                    client.Put(key,value);
                } else if (choice == 2) {
                    // get request
                    System.out.println("Enter key: ");
                    String key = sc.next();
                    sc.nextLine();
                    //System.out.println("Entered key: "+key);
                    String value = (String) client.Get(key);
                    System.out.println("Get request: Key = "+key + " Value = "+value);
                }
            }



        }
    }
}
