package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public abstract class Node extends AbstractActor {
    protected int id;
    protected List<ActorRef> servers;
    protected Map<String, Decision>  decisions =  new HashMap<>();

    public Node(int id) {
        super();
        this.id = id;
    }

    public static class WelcomeMsg implements Serializable {
        public final List<ActorRef> servers;
        public WelcomeMsg(List<ActorRef> servers) {
            this.servers = Collections.unmodifiableList(new ArrayList<>(servers));
        }
    }

    public static class ReadMsg extends Message {
        public final Integer key; // the key of the value to read
        public ReadMsg(String transactionId, int key) {
            super(transactionId);
            this.key = key;
        }
    }

    public static class ReadResultMsg extends Message {
        public final Integer key; // the key associated to the requested item
        public final Integer value; // the value found in the data store for that item
        public ReadResultMsg(String transactionId, int key, int value) {
            super(transactionId);
            this.key = key;
            this.value = value;
        }
    }

    public static class WriteMsg extends Message {
        public final Integer key; // the key of the value to write
        public final Integer value; // the new value to write
        public WriteMsg(String transactionId, int key, int value) {
            super(transactionId);
            this.key = key;
            this.value = value;
        }
    }

    public enum Vote {NO, YES}
    public enum Decision {ABORT, COMMIT}

    public static class VoteRequest extends Message {
        public VoteRequest(String transactionId) {
            super(transactionId);
        }
    }

    public static class VoteResponse extends Message {
        public final Vote vote;
        public VoteResponse(String transactionId, Vote v) {
            super(transactionId);
            vote = v;
        }
    }

    public static class DecisionRequest extends Message {
        public DecisionRequest(String transactionId) {
            super(transactionId);
        }
    }

    public static class DecisionResponse extends Message {
        public final Decision decision;
        public DecisionResponse(String transactionId, Decision d) {
            super(transactionId);
            decision = d;
        }
    }

/*
    public static class Timeout implements Serializable {}

    public static class Recovery implements Serializable {}
*/

    protected void onWelcomeMsg(WelcomeMsg msg) {
        this.servers = msg.servers;
        System.out.println(servers);
    }

/*
    // abstract method to be implemented in extending classes
    protected abstract void onRecovery(Recovery msg);

    // emulate a crash and a recovery in a given time
    void crash(int recoverIn) {
        getContext().become(crashed());
        print("CRASH!!!");

        // setting a timer to "recover"
        getContext().system().scheduler().scheduleOnce(
                Duration.create(recoverIn, TimeUnit.MILLISECONDS),
                getSelf(),
                new Recovery(), // message sent to myself
                getContext().system().dispatcher(), getSelf()
        );
    }

    // emulate a delay of d milliseconds
    void delay(int d) {
        try {Thread.sleep(d);} catch (Exception ignored) {}
    }

    void multicast(Serializable m) {
        for (ActorRef p: servers)
            p.tell(m, getSelf());
    }

    // a multicast implementation that crashes after sending the first message
    void multicastAndCrash(Serializable m, int recoverIn) {
        for (ActorRef p: servers) {
            p.tell(m, getSelf());
            crash(recoverIn); return;
        }
    }

    // schedule a Timeout message in specified time
    void setTimeout(int time) {
        getContext().system().scheduler().scheduleOnce(
                Duration.create(time, TimeUnit.MILLISECONDS),
                getSelf(),
                new Timeout(), // the message to send
                getContext().system().dispatcher(), getSelf()
        );
    }

    // fix the final decision of the current node
    void fixDecision(String transactionId, Decision d) {
        if (!hasDecided(transactionId)) {
            this.decisions.put(transactionId, d);
            print("decided " + d);
        }
    }

    boolean hasDecided(String transactionId) { return decisions.containsKey(transactionId); } // has the node decided?

    // a simple logging function
    void print(String s) {
        System.out.format("%2d: %s\n", id, s);
    }

    public Receive crashed() {
        return receiveBuilder()
                .match(Recovery.class, this::onRecovery)
                .matchAny(msg -> {})
                .build();
    }

    public void onDecisionRequest(DecisionRequest msg) {  */
/* Decision Request *//*

        if (hasDecided(msg.transactionId))
            getSender().tell(new DecisionResponse(msg.transactionId, decisions.get(msg.transactionId)), getSelf());

        // just ignoring if we don't know the decision
    }
*/
}
