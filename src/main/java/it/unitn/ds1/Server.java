package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class Server extends Node {
    private final static int DECISION_TIMEOUT = 2000;  // timeout for the decision, ms
    private final static int N_FAULTY_SERVERS = 1;

    private enum CrashSituation {NONE, BEFORE_VOTING, AFTER_VOTING}
    private final static CrashSituation CRASH_SITUATION = CrashSituation.NONE;

    private final DataEntry[] publicWorkspace = new DataEntry[Main.N_KEYS_PER_SERVER];
    private final Map<String, TransactionInfo> ongoingTransactions = new HashMap<>();

    private static class TransactionInfo {
        public final DataEntry[] privateWorkspace;
        public final ActorRef coordinator;
        public Set<ActorRef> contactedServers;
        public boolean serverHasVoted;
        public TransactionInfo(ActorRef coordinator) {
            this.privateWorkspace = new DataEntry[Main.N_KEYS_PER_SERVER];
            this.coordinator = coordinator;
            this.contactedServers = null;
            this.serverHasVoted = false;
        }
    }

    private static class DataEntry {
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

    /*-- Actor constructor ---------------------------------------------------- */

    public Server(int serverId) {
        super(serverId);
        for (int i = 0; i < Main.N_KEYS_PER_SERVER; i++) {
            publicWorkspace[i] = new DataEntry(0, 100);
        }
    }

    static public Props props(int serverId) {
        return Props.create(Server.class, () -> new Server(serverId));
    }

    /*-- Message classes ------------------------------------------------------ */

    public static class LogRequestMsg implements Serializable {}

    /*-- Actor methods -------------------------------------------------------- */

    private void ensureTransactionIsInitialized(String transactionId, ActorRef coordinator) {
        if (!ongoingTransactions.containsKey(transactionId)) {
            TransactionInfo t = new TransactionInfo(coordinator);
            for (int i = 0; i < Main.N_KEYS_PER_SERVER; i++) {
                t.privateWorkspace[i] = new DataEntry(publicWorkspace[i].version, publicWorkspace[i].value);
            }
            ongoingTransactions.put(transactionId, t);
        }
    }

    private void sendMessageToContactedServersCorrectly(Message msg) {
        TransactionInfo currentTransactionInfo = ongoingTransactions.get(msg.transactionId);
        for(ActorRef server : currentTransactionInfo.contactedServers) {
            sendMessageWithDelay(server, msg);
        }
    }

    /*-- Message handlers ----------------------------------------------------- */

    private void onReadMsg(ReadMsg msg) {
        ActorRef currentCoordinator = getSender();
        ensureTransactionIsInitialized(msg.transactionId, currentCoordinator);

        DataEntry[] privateWorkspace = ongoingTransactions.get(msg.transactionId).privateWorkspace;
        ReadResultMsg readResult = new ReadResultMsg(msg.transactionId, msg.key, privateWorkspace[msg.key % 10].value);
        privateWorkspace[msg.key % 10].readsCounter++;

        sendMessageWithDelay(currentCoordinator, readResult);

        log(msg.transactionId, "Send read result (k:" + readResult.key + ", v:" + readResult.value + ") to coordinator");
    }

    private void onWriteMsg(WriteMsg msg) {
        ensureTransactionIsInitialized(msg.transactionId, getSender());

        DataEntry[] privateWorkspace = ongoingTransactions.get(msg.transactionId).privateWorkspace;
        privateWorkspace[msg.key % 10].value = msg.value;
        privateWorkspace[msg.key % 10].writesCounter++;

        log(msg.transactionId, "Write (k:" + msg.key + ", v:" + privateWorkspace[msg.key % 10].value + ")");
    }

    private void onVoteRequestMsg(VoteRequest msg) {
        if (ongoingTransactions.containsKey(msg.transactionId)) {
            if (CRASH_SITUATION == CrashSituation.BEFORE_VOTING && id < N_FAULTY_SERVERS) { crash(5000); return; }

            TransactionInfo currentTransactionInfo = ongoingTransactions.get(msg.transactionId);
            DataEntry[] privateWorkspace = currentTransactionInfo.privateWorkspace;
            // copying all participant refs except for self
            currentTransactionInfo.contactedServers = msg.contactedServers.stream()
                    .filter((contactedServer) -> !contactedServer.equals(getSelf()))
                    .collect(Collectors.toSet());
            boolean canCommit = true;

            for (int i = 0; i < Main.N_KEYS_PER_SERVER; i++) {
                if (publicWorkspace[i].readsCounter < 0 || publicWorkspace[i].writesCounter < 0) throw new RuntimeException("LOCKS COUNTERS ARE NEGATIVE!");
                log(msg.transactionId, "workspace[" + i + "] -> public = " + publicWorkspace[i] + " | private = " + privateWorkspace[i]);
                // check that versions are the same, only if there were reads or writes
                if ((privateWorkspace[i].readsCounter > 0 || privateWorkspace[i].writesCounter > 0) && !privateWorkspace[i].version.equals(publicWorkspace[i].version)) {
                    log(msg.transactionId, "CANNOT COMMIT: versions are different");
                    canCommit = false;
                    break;
                }
                // if there is a write lock, we cannot read or write
                if (publicWorkspace[i].writesCounter > 0 && (privateWorkspace[i].readsCounter > 0 || privateWorkspace[i].writesCounter > 0)) {
                    log(msg.transactionId, "CANNOT COMMIT: write lock (" + publicWorkspace[i].writesCounter + "), " +
                            "cannot read (" + privateWorkspace[i].readsCounter + ") or write (" + privateWorkspace[i].writesCounter + ")");
                    canCommit = false;
                    break;
                }
                // if there is a read lock, we cannot write
                if (publicWorkspace[i].readsCounter > 0 && privateWorkspace[i].writesCounter > 0) {
                    log(msg.transactionId, "CANNOT COMMIT: read lock (" + publicWorkspace[i].readsCounter + "), cannot write (" + privateWorkspace[i].writesCounter + ")");
                    canCommit = false;
                    break;
                }
            }
            Vote vote;

            if (canCommit) {
                vote = Vote.YES;

                // update locks in the public workspace
                for (int i = 0; i < Main.N_KEYS_PER_SERVER; i++) {
                    publicWorkspace[i].readsCounter += privateWorkspace[i].readsCounter;
                    publicWorkspace[i].writesCounter += privateWorkspace[i].writesCounter;
                }
            } else {
                vote = Vote.NO;

                fixDecision(msg.transactionId, Decision.ABORT);
                ongoingTransactions.remove(msg.transactionId);
            }

            currentTransactionInfo.serverHasVoted = true;

            ActorRef currentCoordinator = getSender();
            sendMessageWithDelay(currentCoordinator, new VoteResponse(msg.transactionId, vote));

            setTimeout(msg.transactionId, DECISION_TIMEOUT);

            log(msg.transactionId, "Can commit? " + canCommit + " -> Vote " + vote);

            if (CRASH_SITUATION == CrashSituation.AFTER_VOTING && id < N_FAULTY_SERVERS) { crash(5000); return; }
        }
    }

    private void onDecisionResponseMsg(DecisionResponse msg) {
        if (ongoingTransactions.containsKey(msg.transactionId)) {
            TransactionInfo currentTransactionInfo = ongoingTransactions.get(msg.transactionId);
            DataEntry[] privateWorkspace = currentTransactionInfo.privateWorkspace;

            fixDecision(msg.transactionId, msg.decision);

            if (msg.decision == Decision.COMMIT) {
                // update values that were written
                for (int i = 0; i < Main.N_KEYS_PER_SERVER; i++) {
                    if (privateWorkspace[i].writesCounter > 0) {
                        publicWorkspace[i].version++;
                        publicWorkspace[i].value = privateWorkspace[i].value;
                    }
                }
            }

            if (currentTransactionInfo.serverHasVoted) {
                //remove locks in the public workspace
                for (int i = 0; i < Main.N_KEYS_PER_SERVER; i++) {
                    publicWorkspace[i].readsCounter -= privateWorkspace[i].readsCounter;
                    publicWorkspace[i].writesCounter -= privateWorkspace[i].writesCounter;
                }
            }

            ongoingTransactions.remove(msg.transactionId);

            log(msg.transactionId, "Final decision from " + getActorName(getSender()) + ": " + msg.decision);
        }
    }

    @Override
    protected void onRecovery(Recovery msg) {
        getContext().become(createReceive());

        Iterator<Map.Entry<String, TransactionInfo>> it = ongoingTransactions.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, TransactionInfo> ongoingTransaction = it.next();
            // for sure the decision is not known yet, otherwise the current transaction would have been already removed from `ongoingTransactions`
            String transactionId = ongoingTransaction.getKey();
            TransactionInfo currentTransactionInfo = ongoingTransactions.get(transactionId);

            if (!currentTransactionInfo.serverHasVoted) { // "the server has not voted" case
                fixDecision(transactionId, Decision.ABORT);
                it.remove();
                log(transactionId, "RECOVERING: not voted yet, aborting transaction");
            } else { // the server has voted YES
                sendMessageWithDelay(currentTransactionInfo.coordinator, new DecisionRequest(transactionId));
                setTimeout(transactionId, DECISION_TIMEOUT);
                log(transactionId, "RECOVERING: voted yes, asking coordinator the decision for transaction");
            }
        }
    }

    private void onTimeout(Timeout msg) {
        if (!hasDecided(msg.transactionId)) {  // Termination protocol
            log(msg.transactionId, "TIMEOUT: decision response not arrived, starting termination protocol...");
            // ask other contacted servers
            sendMessageToContactedServersCorrectly(new DecisionRequest(msg.transactionId));

            // ask also the coordinator
            TransactionInfo currentTransactionInfo = ongoingTransactions.get(msg.transactionId);
            sendMessageWithDelay(currentTransactionInfo.coordinator, new DecisionRequest(msg.transactionId));
            setTimeout(msg.transactionId, DECISION_TIMEOUT);
        }
    }

    private void onLogRequestMsg(LogRequestMsg msg){
        int sum = 0;
        for (int i = 0; i < Main.N_KEYS_PER_SERVER; i++) {
            sum += publicWorkspace[i].value;
        }
        log("FINAL SUM " + sum);
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

    @Override
    protected Receive crashed() {
        return receiveBuilder()
                .match(LogRequestMsg.class, this::onLogRequestMsg)
                .match(Recovery.class, this::onRecovery)
                .matchAny(msg -> {})
                .build();
    }
}
