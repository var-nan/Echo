package main.java.interfaces.Inters;

import java.util.List;

/**
 * @author nandhan, Created on 04/11/23
 */
public interface Replica{

    /**
     * Init method when Replica is created.
     *
     * Connects to Zookeeper server, creates ephemeral node in server,
     * registers a watch event for master node.
     */
    public default void init() {

    }

    /**
     * Create master node in the server.
     */
    public default void runForMaster() {

    }
    //public boolean vote(long commitNumber);

    /**
     * Stores the value in the data-store if the value's version is greater than
     * local version number.
     *
     * All writes are happen through master.
     * @param key
     * @param value
     */
    public void put(String key, Value value);

    /**
     * Returns data that matches with atleast expected version.
     *
     * If local version is too old, it waits until the local data is updated.
     * @param key
     * @param version
     * @return
     */
    public Record get(String key, int version);

    //  master functions
    //public void broadcast(String key, Value value);
    //public List<Replica> getActiveReplicas();
}
