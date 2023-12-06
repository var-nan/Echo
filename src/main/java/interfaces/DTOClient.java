package main.java.interfaces;

import java.io.Serializable;

/**
 * @author nandhan, Created on 25/11/23
 */
public class DTOClient implements Serializable {

    public enum Type implements Serializable{
        GET,
        PUT
    }

    public enum RequestStatus implements Serializable {
        OK,
        NOT_FOUND
    }

    public String key;
    public Object value;
    public Type requestType;
    public RequestStatus requestStatus;

    DTOClient(String key, Object value, Type type) {
        this.key = key;
        this.requestType = type;
        this.value = value;
    }
}
