package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.*;

public class Server extends Node {

    private final DataEntry[] globalWorkspace = new DataEntry[Main.N_KEYS_PER_SERVER];
    private final Map<String, Transaction> transactions = new HashMap<>();

    public static class Transaction {
        DataEntry[] localWorkspace;
        Set<ActorRef> contactedServers;
        Vote vote;
        public Transaction() {
            this.localWorkspace = new DataEntry[Main.N_KEYS_PER_SERVER];
            this.contactedServers = new HashSet<>();
            this.vote = null;
        }
    }

    public static class DataEntry {
        public Integer version;
        public Integer value;
        public Integer readsCounter;
        public Integer writesCounter;
        public DataEntry(int version, int value) {
            this.version = version;
            this.value = value;
            this.readsCounter = 0;
            this.writesCounter = 0;
        }

        @Override
        public String toString() {
            return "(" + version + "," + value + "," + readsCounter + "," + writesCounter + ")";
        }
    }

    public Server(int serverId) {
        super(serverId);
        for (int i = 0; i < Main.N_KEYS_PER_SERVER; i++) {
            globalWorkspace[i] = new DataEntry(0, 100);
        }
    }

    static public Props props(int serverId) {
        return Props.create(Server.class, () -> new Server(serverId));
    }

    public static class LogRequestMsg implements Serializable {}

    private void onReadMsg(ReadMsg msg) {
        ensureLocalWorkspaceExists(msg.transactionId);

        DataEntry[] localWorkspace = transactions.get(msg.transactionId).localWorkspace;
        ReadResultMsg readResult = new ReadResultMsg(msg.transactionId, msg.key, localWorkspace[msg.key % 10].value);
        localWorkspace[msg.key % 10].readsCounter++;

        ActorRef currentCoordinator = getSender();
        currentCoordinator.tell(readResult, getSelf());

        System.out.println("SERVER " + id + " SEND READ RESULT (" + readResult.key + ", " + readResult.value + ") TO COORDINATOR");
    }

    private void onWriteMsg(WriteMsg msg) {
        ensureLocalWorkspaceExists(msg.transactionId);

        DataEntry[] localWorkspace = transactions.get(msg.transactionId).localWorkspace;
        localWorkspace[msg.key % 10].value = msg.value;
        localWorkspace[msg.key % 10].writesCounter++;

        System.out.println("SERVER " + id + " WRITE (" + msg.key + ", " + localWorkspace[msg.key % 10].value + ")");
    }

    private void onVoteRequestMsg(VoteRequest msg) {
        Transaction currentTransaction = transactions.get(msg.transactionId);
        DataEntry[] localWorkspace = currentTransaction.localWorkspace;
        currentTransaction.contactedServers = msg.contactedServers;
        boolean canCommit = true;

        for (int i = 0; i < Main.N_KEYS_PER_SERVER; i++) {
            if (globalWorkspace[i].readsCounter < 0 || globalWorkspace[i].writesCounter < 0) throw new RuntimeException("LOCKS COUNTERS ARE NEGATIVE!");
            System.out.println("SERVER " + id + "." + i + " global = " + globalWorkspace[i] + " | local = " + localWorkspace[i]);
            // check that versions are the same, only if there were reads or writes
            if ((localWorkspace[i].readsCounter > 0 || localWorkspace[i].writesCounter > 0) && !localWorkspace[i].version.equals(globalWorkspace[i].version)) {
                System.out.println("SERVER " + id + " VERSIONS ARE DIFFERENT");
                canCommit = false;
                break;
            }
            // if there is a write lock, we cannot read or write
            if (globalWorkspace[i].writesCounter > 0 && (localWorkspace[i].readsCounter > 0 || localWorkspace[i].writesCounter > 0)) {
                System.out.println("SERVER " + id + " WRITE LOCK (" + globalWorkspace[i].writesCounter + "), " +
                        "CANNOT READ (" + localWorkspace[i].readsCounter + ") OR WRITE(" + localWorkspace[i].writesCounter + ")");
                canCommit = false;
                break;
            }
            // if there is a read lock, we cannot write
            if (globalWorkspace[i].readsCounter > 0 && localWorkspace[i].writesCounter > 0) {
                System.out.println("SERVER " + id + " READ LOCK, CANNOT WRITE");
                canCommit = false;
                break;
            }
        }

        System.out.println("SERVER " + id + " CAN COMMIT? " + canCommit);

        Vote vote;

        // update locks in the global workspace
        if (canCommit) {
            for (int i = 0; i < Main.N_KEYS_PER_SERVER; i++) {
                globalWorkspace[i].readsCounter += localWorkspace[i].readsCounter;
                globalWorkspace[i].writesCounter += localWorkspace[i].writesCounter;
            }
            vote = Vote.YES;
        } else {
            vote = Vote.NO;
        }

        currentTransaction.vote = vote;

        ActorRef currentCoordinator = getSender();
        currentCoordinator.tell(new VoteResponse(msg.transactionId, vote), getSelf());

        System.out.println("SERVER " + id + " VOTE " + vote);
    }

    private void onDecisionResponseMsg(DecisionResponse msg) {
        Transaction currentTransaction = transactions.get(msg.transactionId);
        DataEntry[] localWorkspace = currentTransaction.localWorkspace;

        if (currentTransaction.vote == Vote.YES) {
            //remove locks in the global workspace
            for (int i = 0; i < Main.N_KEYS_PER_SERVER; i++) {
                globalWorkspace[i].readsCounter -= localWorkspace[i].readsCounter;
                globalWorkspace[i].writesCounter -= localWorkspace[i].writesCounter;
            }
        }

        if (msg.decision == Decision.COMMIT) {
            // update values that were written
            for (int i = 0; i < Main.N_KEYS_PER_SERVER; i++) {
                if (localWorkspace[i].writesCounter > 0) {
                    globalWorkspace[i].version++;
                    globalWorkspace[i].value = localWorkspace[i].value;
                }
            }
        }

        transactions.remove(msg.transactionId);

        System.out.println("SERVER " + id + " FINAL DECISION " + msg.decision);
    }

    private void onLogRequestMsg(LogRequestMsg msg) {
        int sum = 0;
        for (int i = 0; i < Main.N_KEYS_PER_SERVER; i++) {
            sum += globalWorkspace[i].value;
        }
        System.out.println("SERVER " + id + " FINAL SUM " + sum);
    }

    private void ensureLocalWorkspaceExists(String transactionId) {
        if (!transactions.containsKey(transactionId)) {
            Transaction t = new Transaction();
            for (int i = 0; i < Main.N_KEYS_PER_SERVER; i++) {
                t.localWorkspace[i] = new DataEntry(globalWorkspace[i].version, globalWorkspace[i].value);
            }
            transactions.put(transactionId, t);
        }
    }

    @Override
    public AbstractActor.Receive createReceive() {
        return receiveBuilder()
                .match(WelcomeMsg.class, this::onWelcomeMsg)
                .match(ReadMsg.class,  this::onReadMsg)
                .match(WriteMsg.class, this::onWriteMsg)
                .match(VoteRequest.class, this::onVoteRequestMsg)
                .match(DecisionResponse.class, this::onDecisionResponseMsg)
                .match(LogRequestMsg.class, this::onLogRequestMsg)
                .build();
    }
}
