package main.java.interfaces.Inters;

import java.io.Serializable;

/**
 * @author nandhan, Created on 24/11/23
 */
public class DTO {

    class ClientDto implements Serializable {
        String key;
        Object value;
    }

    enum COMMIT_RESPONSE implements Serializable {
        OLD_COMMIT ,
        ACKNOWLEDGEMENT,
        DONE
    }

    /**
     * @author nandhan, Created on 23/11/23
     */
    static class KVPair implements Serializable {
        /*
        Object of this class is used as DTO.
         */
        String key;
        Object value;

        long commitId;

        KVPair(String key, Object value) {
            this.key = key;
            this.value = value;
            this.commitId = -1;
        }

        KVPair(String key, Object value, long commitId) {
            this.key = key;
            this.value = value;
            this.commitId = commitId;
        }
    }

    /**
     * @author nandhan, Created on 24/11/23
     */
    static class RequestObject implements Serializable {
        String replicaId;
        DTO.KVPair KVPair;

        RequestObject(String replicaNumber, DTO.KVPair KVPair) {
            this.replicaId = replicaNumber;
            this.KVPair = KVPair;
        }
    }
}
