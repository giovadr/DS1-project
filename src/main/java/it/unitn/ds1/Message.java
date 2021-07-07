package it.unitn.ds1;

import java.io.Serializable;

public class Message implements Serializable {
    public final String transactionId;

    public Message(String transactionId) {
        this.transactionId = transactionId;
    }
}
