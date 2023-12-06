package main.java.interfaces.Inters;

/**
 * @author nandhan, Created on 22/11/23
 */
public class DemoThread extends Thread{


    //@Override
    //public void start() {
    //    System.out.println("Start method is called.");
    //    run();
    //}

    @Override
    public void run() {
        System.out.println("Run method is called.");
    }

    public static void main(String[] args) {
        DemoThread thread = new DemoThread();

        thread.start();
        //thread.run();
    }
}
