package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.*;

public class Server extends Node {
    final static int DECISION_TIMEOUT = 2000;  // timeout for the decision, ms

    private final DataEntry[] globalWorkspace = new DataEntry[Main.N_KEYS_PER_SERVER];
    private final Map<String, TransactionInfo> ongoingTransactions = new HashMap<>();

    private static class TransactionInfo {
        DataEntry[] localWorkspace;
        ActorRef coordinator;
        Set<ActorRef> contactedServers;
        Vote vote;
        public TransactionInfo() {
            this.localWorkspace = new DataEntry[Main.N_KEYS_PER_SERVER];
            this.coordinator = null;
            this.contactedServers = null;
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
        ActorRef currentCoordinator = getSender();
        ensureTransactionIsInitialized(msg.transactionId, currentCoordinator);

        DataEntry[] localWorkspace = ongoingTransactions.get(msg.transactionId).localWorkspace;
        ReadResultMsg readResult = new ReadResultMsg(msg.transactionId, msg.key, localWorkspace[msg.key % 10].value);
        localWorkspace[msg.key % 10].readsCounter++;

        currentCoordinator.tell(readResult, getSelf());

        System.out.println("SERVER " + id + " SEND READ RESULT (" + readResult.key + ", " + readResult.value + ") TO COORDINATOR");
    }

    private void onWriteMsg(WriteMsg msg) {
        ensureTransactionIsInitialized(msg.transactionId, getSender());

        DataEntry[] localWorkspace = ongoingTransactions.get(msg.transactionId).localWorkspace;
        localWorkspace[msg.key % 10].value = msg.value;
        localWorkspace[msg.key % 10].writesCounter++;

        System.out.println("SERVER " + id + " WRITE (" + msg.key + ", " + localWorkspace[msg.key % 10].value + ")");
    }

    private void onVoteRequestMsg(VoteRequest msg) {
        TransactionInfo currentTransactionInfo = ongoingTransactions.get(msg.transactionId);
        DataEntry[] localWorkspace = currentTransactionInfo.localWorkspace;
        currentTransactionInfo.contactedServers = msg.contactedServers;
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

            fixDecision(msg.transactionId, Decision.ABORT);
            ongoingTransactions.remove(msg.transactionId);
        }

        currentTransactionInfo.vote = vote;

        ActorRef currentCoordinator = getSender();
        currentCoordinator.tell(new VoteResponse(msg.transactionId, vote), getSelf());

        setTimeout(msg.transactionId, DECISION_TIMEOUT);

        System.out.println("SERVER " + id + " VOTE " + vote);
    }

    private void onDecisionResponseMsg(DecisionResponse msg) {
        if (ongoingTransactions.containsKey(msg.transactionId)) {
            TransactionInfo currentTransactionInfo = ongoingTransactions.get(msg.transactionId);
            DataEntry[] localWorkspace = currentTransactionInfo.localWorkspace;

            fixDecision(msg.transactionId, msg.decision);

            if (msg.decision == Decision.COMMIT) {
                // update values that were written
                for (int i = 0; i < Main.N_KEYS_PER_SERVER; i++) {
                    if (localWorkspace[i].writesCounter > 0) {
                        globalWorkspace[i].version++;
                        globalWorkspace[i].value = localWorkspace[i].value;
                    }
                }
            }

            if (currentTransactionInfo.vote == Vote.YES) {
                //remove locks in the global workspace
                for (int i = 0; i < Main.N_KEYS_PER_SERVER; i++) {
                    globalWorkspace[i].readsCounter -= localWorkspace[i].readsCounter;
                    globalWorkspace[i].writesCounter -= localWorkspace[i].writesCounter;
                }
            }

            ongoingTransactions.remove(msg.transactionId);

            System.out.println("SERVER " + id + " FINAL DECISION " + msg.decision);
        }
    }

    @Override
    public void onRecovery(Recovery msg) {
        // TODO: gestire i crash del server
    }

    public void onTimeout(Timeout msg) {
        if (!hasDecided(msg.transactionId)) {  // Termination protocol
            // ask other contacted servers
            sendMessageCorrectlyToAllContactedServers(new DecisionRequest(msg.transactionId));

            // ask also the coordinator
            TransactionInfo currentTransactionInfo = ongoingTransactions.get(msg.transactionId);
            currentTransactionInfo.coordinator.tell(new DecisionRequest(msg.transactionId), getSelf());
            setTimeout(msg.transactionId, DECISION_TIMEOUT);
        }
    }

    private void onLogRequestMsg(LogRequestMsg msg) {
        int sum = 0;
        for (int i = 0; i < Main.N_KEYS_PER_SERVER; i++) {
            sum += globalWorkspace[i].value;
        }
        System.out.println("SERVER " + id + " FINAL SUM " + sum);
    }

    private void ensureTransactionIsInitialized(String transactionId, ActorRef coordinator) {
        if (!ongoingTransactions.containsKey(transactionId)) {
            TransactionInfo t = new TransactionInfo();
            for (int i = 0; i < Main.N_KEYS_PER_SERVER; i++) {
                t.localWorkspace[i] = new DataEntry(globalWorkspace[i].version, globalWorkspace[i].value);
            }
            t.coordinator = coordinator;
            ongoingTransactions.put(transactionId, t);
        }
    }

    // TODO: spostare nella classe Node e fare refactoring
    private void sendMessageCorrectlyToAllContactedServers(Message msg) {
        TransactionInfo currentTransactionInfo = ongoingTransactions.get(msg.transactionId);
        for(ActorRef server : currentTransactionInfo.contactedServers) {
            server.tell(msg, getSelf());
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
                .match(Timeout.class, this::onTimeout)
                .match(DecisionRequest.class, this::onDecisionRequest)
                .build();
    }
}
