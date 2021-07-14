package it.unitn.ds1;

import akka.actor.*;

import java.io.Serializable;
import java.util.*;

public class Coordinator extends Node {
    private final static int VOTE_TIMEOUT = 1000;
    private final static int FAULTY_COORDINATOR_ID = 0;

    private enum CrashType {NONE, BEFORE_ANY_SEND, AFTER_FIRST_SEND, AFTER_ALL_SENDS}
    private final static CrashType CRASH_DURING_VOTE_REQUEST = CrashType.NONE;
    private final static CrashType CRASH_DURING_SEND_DECISION = CrashType.NONE;

    private Integer transactionsCounter;
    private final Map<String, TransactionInfo> ongoingTransactions = new HashMap<>();
    private final Map<ActorRef, String> transactionIdForClients = new HashMap<>();

    public Coordinator(int coordinatorId) {
        super(coordinatorId);
        this.transactionsCounter = 0;
    }

    static public Props props(int coordinatorId) {
        return Props.create(Coordinator.class, () -> new Coordinator(coordinatorId));
    }

    private static class TransactionInfo {
        public final ActorRef client;
        public final Set<ActorRef> contactedServers;
        public Integer nYesVotes;
        public TransactionInfo(ActorRef client) {
            this.client = client;
            this.contactedServers = new HashSet<>();
            this.nYesVotes = 0;
        }

        public boolean everyoneVotedYes() {
            return nYesVotes == contactedServers.size();
        }
    }

    private void onTxnBeginMsg(Client.TxnBeginMsg msg) {
        ActorRef currentClient = getSender();
        sendMessageToClient(currentClient, new Client.TxnAcceptMsg());
        initializeTransaction(currentClient);
    }

    private void onReadMsg(Client.ReadMsg msg) {
        ActorRef currentClient = getSender();
        String transactionId = transactionIdForClients.get(currentClient);
        ActorRef currentServer = getServerFromKey(msg.key);
        currentServer.tell(new ReadMsg(transactionId, msg.key), getSelf());
        addContactedServer(transactionId, currentServer);
        setTimeout(transactionId, VOTE_TIMEOUT);
    }

    private void onReadResultMsg(ReadResultMsg msg) {
        ActorRef currentClient = ongoingTransactions.get(msg.transactionId).client;
        sendMessageToClient(currentClient, new Client.ReadResultMsg(msg.key, msg.value));

        log(msg.transactionId, "Send read result (k:" + msg.key + ", v:" + msg.value + ") to client");
    }

    private void onWriteMsg(Client.WriteMsg msg) {
        ActorRef currentClient = getSender();
        String transactionId = transactionIdForClients.get(currentClient);
        ActorRef currentServer = getServerFromKey(msg.key);

        currentServer.tell(new WriteMsg(transactionId, msg.key, msg.value), getSelf());
        addContactedServer(transactionId, currentServer);

        log(transactionId, "Write (k:" + msg.key + ", v:" + msg.value + ")");
    }

    private void onTxnEndMsg(Client.TxnEndMsg msg) {
        ActorRef currentClient = getSender();
        String transactionId = transactionIdForClients.get(currentClient);

        log(transactionId, "End of transaction, client voted " + msg.commit);

        if (msg.commit) {
            sendVoteRequestToContactedServersSimulatingCrash(transactionId);
            setTimeout(transactionId, VOTE_TIMEOUT);
        } else {
            fixDecision(transactionId, Decision.ABORT);
            sendDecisionToClientAndContactedServersSimulatingCrash(transactionId);
        }
    }

    private void onVoteResponseMsg(VoteResponse msg) {
        TransactionInfo currentTransactionInfo = ongoingTransactions.get(msg.transactionId);

        if (currentTransactionInfo != null) {
            if (msg.vote == Vote.YES) {
                currentTransactionInfo.nYesVotes++;

                if (currentTransactionInfo.everyoneVotedYes()) {
                    log(msg.transactionId, "COMMIT: everyone voted yes");
                    fixDecision(msg.transactionId, Decision.COMMIT);
                    sendDecisionToClientAndContactedServersSimulatingCrash(msg.transactionId);
                }
            } else {
                log(msg.transactionId, "ABORT: a server voted no");
                fixDecision(msg.transactionId, Decision.ABORT);
                sendDecisionToClientAndContactedServersSimulatingCrash(msg.transactionId);
            }
        }
    }

    @Override
    protected void onRecovery(Recovery msg) {
        getContext().become(createReceive());

        // the following copy is needed to avoid `ConcurrentModificationException`, since we modify the map while iterating it
        Map<String, TransactionInfo> ongoingTransactionsCopy = new HashMap<>(ongoingTransactions);

        for (Map.Entry<String, TransactionInfo> ongoingTransaction: ongoingTransactionsCopy.entrySet()){
            String transactionId = ongoingTransaction.getKey();
            if(!hasDecided(transactionId)) {
                fixDecision(transactionId, Decision.ABORT);
                log(transactionId, "RECOVERING: not decided yet, aborting...");
            } else {
                log(transactionId, "RECOVERING: already decided, communicating decision to others...");
            }

            sendDecisionToClientAndContactedServersCorrectly(transactionId);
        }
    }

    private void onTimeout(Timeout msg) {
        if (!hasDecided(msg.transactionId)) {
            // not decided in time means ABORT
            fixDecision(msg.transactionId, Decision.ABORT);
            sendDecisionToClientAndContactedServersCorrectly(msg.transactionId);
            log(msg.transactionId, "TIMEOUT: one or more messages were lost, aborting...");
        }
    }

    private void initializeTransaction(ActorRef client) {
        String transactionId = id + "." + transactionsCounter;
        ongoingTransactions.put(transactionId, new TransactionInfo(client));
        transactionIdForClients.put(client, transactionId);
        transactionsCounter++;
    }

    private void addContactedServer(String transactionId, ActorRef server) {
        ongoingTransactions.get(transactionId).contactedServers.add(server);
    }

    private ActorRef getServerFromKey(int key) {
        int serverId = key / 10;
        return servers.get(serverId);
    }

    private void sendMessageToClient(ActorRef client, Serializable msg) {
        client.tell(msg, getSelf());
    }

    private void sendVoteRequestToContactedServersSimulatingCrash(String transactionId) {
        TransactionInfo currentTransactionInfo = ongoingTransactions.get(transactionId);
        Message msg = new VoteRequest(transactionId, currentTransactionInfo.contactedServers);

        sendMessageToContactedServersSimulatingCrash(msg, CRASH_DURING_VOTE_REQUEST);
    }

    private void sendDecisionToClientAndContactedServersCorrectly(String transactionId) {
        sendDecisionToClientAndContactedServersSimulatingCrash(transactionId, CrashType.NONE);
    }

    private void sendDecisionToClientAndContactedServersSimulatingCrash(String transactionId) {
        sendDecisionToClientAndContactedServersSimulatingCrash(transactionId, CRASH_DURING_SEND_DECISION);
    }

    private void sendDecisionToClientAndContactedServersSimulatingCrash(String transactionId, CrashType crashType) {
        Decision decision = decisions.get(transactionId);
        Message msg = new DecisionResponse(transactionId, decision);

        sendMessageToContactedServersSimulatingCrash(msg, crashType);

        if (crashType == CrashType.NONE) {
            TransactionInfo currentTransactionInfo = ongoingTransactions.get(transactionId);
            sendMessageToClient(currentTransactionInfo.client, new Client.TxnResultMsg(decision == Decision.COMMIT));
            ongoingTransactions.remove(msg.transactionId);
        }
    }

    private void sendMessageToContactedServersSimulatingCrash(Message msg, CrashType crashType) {
        TransactionInfo currentTransactionInfo = ongoingTransactions.get(msg.transactionId);

        if (crashType == CrashType.BEFORE_ANY_SEND && id == FAULTY_COORDINATOR_ID) {
            crash(5000);
            return;
        }

        for(ActorRef server : currentTransactionInfo.contactedServers) {
            server.tell(msg, getSelf());

            if (crashType == CrashType.AFTER_FIRST_SEND && id == FAULTY_COORDINATOR_ID) {
                crash(3000);
                return;
            }
        }

        if (crashType == CrashType.AFTER_ALL_SENDS && id == FAULTY_COORDINATOR_ID) {
            crash(5000);
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(WelcomeMsg.class, this::onWelcomeMsg)
                .match(Client.TxnBeginMsg.class, this::onTxnBeginMsg)
                .match(Client.ReadMsg.class, this::onReadMsg)
                .match(ReadResultMsg.class, this::onReadResultMsg)
                .match(Client.WriteMsg.class, this::onWriteMsg)
                .match(Client.TxnEndMsg.class, this::onTxnEndMsg)
                .match(VoteResponse.class, this::onVoteResponseMsg)
                .match(Timeout.class, this::onTimeout)
                .match(DecisionRequest.class, this::onDecisionRequest)
                .build();
    }

    @Override
    protected Receive crashed() {
        return receiveBuilder()
                .match(Recovery.class, this::onRecovery)
                .matchAny(msg -> {})
                .build();
    }
}
