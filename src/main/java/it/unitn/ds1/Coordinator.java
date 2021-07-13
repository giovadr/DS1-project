package it.unitn.ds1;

import akka.actor.*;

import java.io.Serializable;
import java.util.*;

public class Coordinator extends Node {
    final static int VOTE_TIMEOUT = 1000;
    final static int FAULTY_COORDINATOR_ID = 0;

    public enum CrashType {NONE, AFTER_FIRST_SEND, AFTER_ALL_SENDS}
    final static CrashType CRASH_DURING_VOTE_REQUEST = CrashType.NONE;
    final static CrashType CRASH_DURING_SEND_DECISION = CrashType.NONE;

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
    }

    private void onReadResultMsg(ReadResultMsg msg) {
        ActorRef currentClient = ongoingTransactions.get(msg.transactionId).client;
        sendMessageToClient(currentClient, new Client.ReadResultMsg(msg.key, msg.value));

        log("Send read result (k:" + msg.key + ", v:" + msg.value + ") to client");
    }

    private void onWriteMsg(Client.WriteMsg msg) {
        ActorRef currentClient = getSender();
        String transactionId = transactionIdForClients.get(currentClient);
        ActorRef currentServer = getServerFromKey(msg.key);

        currentServer.tell(new WriteMsg(transactionId, msg.key, msg.value), getSelf());
        addContactedServer(transactionId, currentServer);

        log("Write (k:" + msg.key + ", v:" + msg.value + ")");
    }

    private void onTxnEndMsg(Client.TxnEndMsg msg) {
        ActorRef currentClient = getSender();
        String transactionId = transactionIdForClients.get(currentClient);
        TransactionInfo currentTransactionInfo = ongoingTransactions.get(transactionId);

        log("End of transaction, client voted " + msg.commit);

        if (msg.commit) {
            sendMessageSimulatingCrash(transactionId,
                    new VoteRequest(transactionId, currentTransactionInfo.contactedServers),
                    CRASH_DURING_VOTE_REQUEST);
            setTimeout(transactionId, VOTE_TIMEOUT);
        } else {
            fixDecision(transactionId, Decision.ABORT);
            sendMessageSimulatingCrash(transactionId,
                    new DecisionResponse(transactionId, Decision.ABORT),
                    CRASH_DURING_SEND_DECISION);
        }
    }

    private void onVoteResponseMsg(VoteResponse msg) {
        TransactionInfo currentTransactionInfo = ongoingTransactions.get(msg.transactionId);

        if (currentTransactionInfo != null) {
            if (msg.vote == Vote.YES) {
                currentTransactionInfo.nYesVotes++;

                if (currentTransactionInfo.everyoneVotedYes()) {
                    fixDecision(msg.transactionId, Decision.COMMIT);
                    sendMessageSimulatingCrash(msg.transactionId,
                            new DecisionResponse(msg.transactionId, Decision.COMMIT),
                            CRASH_DURING_SEND_DECISION);
                    log("COMMIT: everyone voted yes");
                }
            } else {
                fixDecision(msg.transactionId, Decision.ABORT);
                sendMessageSimulatingCrash(msg.transactionId,
                        new DecisionResponse(msg.transactionId, Decision.ABORT),
                        CRASH_DURING_SEND_DECISION);
                log("ABORT: a server voted no");
            }
        }
    }

    @Override
    public void onRecovery(Recovery msg) {
        getContext().become(createReceive());

        Iterator<Map.Entry<String, TransactionInfo>> it = ongoingTransactions.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, TransactionInfo> ongoingTransaction = it.next();
            String transactionId = ongoingTransaction.getKey();
            if(!hasDecided(transactionId)) {
                fixDecision(transactionId, Decision.ABORT);
                log("RECOVERING: not decided yet, aborting transaction " + transactionId + "...");
            } else {
                log("RECOVERING: already decided, communicating decision for transaction " + transactionId + "...");
            }

            Decision decision = decisions.get(transactionId);
            communicateDecisionToClientAndAllContactedServers(transactionId, decision);
            it.remove();
        }
    }

    public void onTimeout(Timeout msg) {
        if (!hasDecided(msg.transactionId)) {
            // not decided in time means ABORT
            fixDecision(msg.transactionId, Decision.ABORT);
            communicateDecisionToClientAndAllContactedServers(msg.transactionId, Decision.ABORT);
            ongoingTransactions.remove(msg.transactionId);
            log("TIMEOUT: one or more votes were lost, aborting...");
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

    private void communicateDecisionToClientAndAllContactedServers(String transactionId, Decision decision) {
        TransactionInfo currentTransactionInfo = ongoingTransactions.get(transactionId);
        sendMessageCorrectlyToAllContactedServers(new DecisionResponse(transactionId, decision));
        sendMessageToClient(currentTransactionInfo.client, new Client.TxnResultMsg(decision == Decision.COMMIT));
    }

    private void sendMessageCorrectlyToAllContactedServers(Message msg) {
        TransactionInfo currentTransactionInfo = ongoingTransactions.get(msg.transactionId);
        for(ActorRef server : currentTransactionInfo.contactedServers) {
            server.tell(msg, getSelf());
        }
    }

    private void sendMessageToClient(ActorRef client, Serializable msg) {
        client.tell(msg, getSelf());
    }

    private void sendMessageSimulatingCrash(String transactionId, Message msg, CrashType crashType) {
        switch (crashType) {
            case NONE:
                if (msg instanceof VoteRequest) {
                    sendMessageCorrectlyToAllContactedServers(msg);
                } else if (msg instanceof DecisionResponse) {
                    communicateDecisionToClientAndAllContactedServers(transactionId, ((DecisionResponse) msg).decision);
                    ongoingTransactions.remove(msg.transactionId);
                }
                break;
            case AFTER_FIRST_SEND:
                sendMessageToFirstContactedServerAndThenCrash(msg, 3000);
                break;
            case AFTER_ALL_SENDS:
                sendMessageCorrectlyToAllContactedServers(msg);
                crash(5000, FAULTY_COORDINATOR_ID);
                break;
        }
    }

    private void sendMessageToFirstContactedServerAndThenCrash(Message msg, int recoverIn) {
        TransactionInfo currentTransactionInfo = ongoingTransactions.get(msg.transactionId);
        for(ActorRef server : currentTransactionInfo.contactedServers) {
            server.tell(msg, getSelf());
            crash(recoverIn, FAULTY_COORDINATOR_ID);
            return;
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
}
