package it.unitn.ds1;

import akka.actor.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class Coordinator extends AbstractActor{

    private final Integer coordinatorId;
    private List<ActorRef> servers;

    public Coordinator(int coordinatorId) {
        this.coordinatorId = coordinatorId;
    }

    static public Props props(int coordinatorId) {
        return Props.create(Coordinator.class, () -> new Coordinator(coordinatorId));
    }

    public static class WelcomeMsg implements Serializable {
        public final List<ActorRef> servers;
        public WelcomeMsg(List<ActorRef> servers) {
            this.servers = Collections.unmodifiableList(new ArrayList<>(servers));
        }
    }

    private void onWelcomeMsg(WelcomeMsg msg) {
        this.servers = msg.servers;
        System.out.println(servers);
    }

    private void onTxnBeginMsg(Client.TxnBeginMsg msg) {
        ActorRef currentClient = getSender();
        currentClient.tell(new Client.TxnAcceptMsg(), getSelf());
    }

    private void onReadMsg(Client.ReadMsg msg) {
        ActorRef currentClient = getSender();
        // TODO: retrieve the value from the server
        Client.ReadResultMsg readResult = new Client.ReadResultMsg(msg.key, 100);
        currentClient.tell(readResult, getSelf());

        System.out.println("COORDINATOR " + coordinatorId + " SEND READ RESULT (" + readResult.key + ", " + readResult.value + ") TO CLIENT " + msg.clientId);
    }

    private void onWriteMsg(Client.WriteMsg msg) {
        // TODO: write value on the server

        System.out.println("COORDINATOR " + coordinatorId + " WRITE (" + msg.key + ", " + msg.value + ")");
    }

    private void onTxnEndMsg(Client.TxnEndMsg msg) {
        ActorRef currentClient = getSender();

        // TODO: decide if commit or abort
        Client.TxnResultMsg transactionResult = new Client.TxnResultMsg(true);
        currentClient.tell(transactionResult, getSelf());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(WelcomeMsg.class,  this::onWelcomeMsg)
                .match(Client.TxnBeginMsg.class,  this::onTxnBeginMsg)
                .match(Client.ReadMsg.class,  this::onReadMsg)
                .match(Client.WriteMsg.class,  this::onWriteMsg)
                .match(Client.TxnEndMsg.class,  this::onTxnEndMsg)
                .build();
    }
}
