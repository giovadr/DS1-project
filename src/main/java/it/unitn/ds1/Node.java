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

    protected static class WelcomeMsg implements Serializable {
        public final List<ActorRef> servers;
        public WelcomeMsg(List<ActorRef> servers) {
            this.servers = Collections.unmodifiableList(new ArrayList<>(servers));
        }
    }

    protected static class ReadMsg extends Message {
        public final Integer key; // the key of the value to read
        public ReadMsg(String transactionId, int key) {
            super(transactionId);
            this.key = key;
        }
    }

    protected static class ReadResultMsg extends Message {
        public final Integer key; // the key associated to the requested item
        public final Integer value; // the value found in the data store for that item
        public ReadResultMsg(String transactionId, int key, int value) {
            super(transactionId);
            this.key = key;
            this.value = value;
        }
    }

    protected static class WriteMsg extends Message {
        public final Integer key; // the key of the value to write
        public final Integer value; // the new value to write
        public WriteMsg(String transactionId, int key, int value) {
            super(transactionId);
            this.key = key;
            this.value = value;
        }
    }

    protected enum Vote {NO, YES}
    protected enum Decision {ABORT, COMMIT}

    protected static class VoteRequest extends Message {
        public final Set<ActorRef> contactedServers;
        public VoteRequest(String transactionId, Set<ActorRef> contactedServers) {
            super(transactionId);
            this.contactedServers = Collections.unmodifiableSet(new HashSet<>(contactedServers));
        }
    }

    protected static class VoteResponse extends Message {
        public final Vote vote;
        public VoteResponse(String transactionId, Vote v) {
            super(transactionId);
            vote = v;
        }
    }

    protected static class DecisionRequest extends Message {
        public DecisionRequest(String transactionId) {
            super(transactionId);
        }
    }

    protected static class DecisionResponse extends Message {
        public final Decision decision;
        public DecisionResponse(String transactionId, Decision d) {
            super(transactionId);
            decision = d;
        }
    }

    protected static class Timeout extends Message {
        public Timeout(String transactionId) {
            super(transactionId);
        }
    }

    protected static class Recovery implements Serializable {}

    protected void onWelcomeMsg(WelcomeMsg msg) {
        this.servers = msg.servers;
        System.out.println(servers);
    }

    // abstract method to be implemented in extending classes
    protected abstract void onRecovery(Recovery msg);

    // emulate a crash and a recovery in a given time
    protected void crash(int recoverIn) {
        getContext().become(crashed());

        // setting a timer to "recover"
        getContext().system().scheduler().scheduleOnce(
                Duration.create(recoverIn, TimeUnit.MILLISECONDS),
                getSelf(),
                new Recovery(), // message sent to myself
                getContext().system().dispatcher(), getSelf()
        );
        log("CRASH!");
    }

    protected void log(String s) {
        log("", s);
    }

    protected void log(String transactionId, String s) {
        if (!transactionId.isEmpty()) {
            transactionId = "[T#" + transactionId + "] ";
        }
        System.out.format("%s[%s] %s\n", transactionId, getActorName(self()), s);
    }

    protected String getActorName(ActorRef actor) {
        return actor.path().name();
    }

    protected void sendMessageWithDelay(ActorRef receiver, Serializable msg) {
        Random r = new Random();
        final int minDelay = 10;
        final int maxDelay = 30;
        // random delay between `minDelay` and `maxDelay` included
        int d = r.nextInt(maxDelay - minDelay + 1) + minDelay;
        delay(d);
        receiver.tell(msg, getSelf());
    }

    // emulate a delay of d milliseconds
    protected void delay(int d) {
        try {Thread.sleep(d);} catch (Exception ignored) {}
    }

    // schedule a Timeout message in specified time
    protected void setTimeout(String transactionId, int time) {
        getContext().system().scheduler().scheduleOnce(
                Duration.create(time, TimeUnit.MILLISECONDS),
                getSelf(),
                new Timeout(transactionId), // the message to send
                getContext().system().dispatcher(), getSelf()
        );
    }

    // fix the final decision of the current node
    protected void fixDecision(String transactionId, Decision d) {
        if (!hasDecided(transactionId)) {
            this.decisions.put(transactionId, d);
        }
    }

    protected boolean hasDecided(String transactionId) {
        return decisions.containsKey(transactionId);
    }

    protected abstract Receive crashed();

    protected void onDecisionRequest(DecisionRequest msg) {
        if (hasDecided(msg.transactionId)) {
            getSender().tell(new DecisionResponse(msg.transactionId, decisions.get(msg.transactionId)), getSelf());
            log(msg.transactionId, "Incoming decision request: decision known -> " + decisions.get(msg.transactionId));
        } else {
            // just ignoring if we don't know the decision
            log(msg.transactionId, "Incoming decision request: decision unknown");
        }
    }
}
