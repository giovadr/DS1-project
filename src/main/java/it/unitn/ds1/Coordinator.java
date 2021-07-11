package it.unitn.ds1;

import akka.actor.*;

import java.io.Serializable;
import java.util.*;

public class Coordinator extends Node {
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
        public boolean isTransactionEnded;
        public TransactionInfo(ActorRef client) {
            this.client = client;
            this.contactedServers = new HashSet<>();
            this.nYesVotes = 0;
            this.isTransactionEnded = false;
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

        System.out.println("COORDINATOR " + id + " SEND READ RESULT (" + msg.key + ", " + msg.value + ") TO " + currentClient.path().name());
    }

    private void onWriteMsg(Client.WriteMsg msg) {
        ActorRef currentClient = getSender();
        String transactionId = transactionIdForClients.get(currentClient);
        ActorRef currentServer = getServerFromKey(msg.key);

        currentServer.tell(new WriteMsg(transactionId, msg.key, msg.value), getSelf());
        addContactedServer(transactionId, currentServer);

        System.out.println("COORDINATOR " + id + " WRITE (" + msg.key + ", " + msg.value + ")");
    }

    private void onTxnEndMsg(Client.TxnEndMsg msg) {
        ActorRef currentClient = getSender();
        String transactionId = transactionIdForClients.get(currentClient);
        TransactionInfo currentTransactionInfo = ongoingTransactions.get(transactionId);

        currentTransactionInfo.isTransactionEnded = true;

        if (msg.commit) {
            sendMessageToAllContactedServersSimulatingCrash(
                    new VoteRequest(transactionId, currentTransactionInfo.contactedServers),
                    CRASH_DURING_VOTE_REQUEST
            );
        } else {
            // TODO: bisogna simulare i crash qui? Chiedere all'esercitatore
            fixAndCommunicateDecisionSimulatingCrash(transactionId, Decision.ABORT);
        }

        System.out.println("COORDINATOR " + id + " END");
    }

    private void onVoteResponseMsg(VoteResponse msg) {
        TransactionInfo currentTransactionInfo = ongoingTransactions.get(msg.transactionId);

        if (currentTransactionInfo != null) {
            if (msg.vote == Vote.YES) {
                currentTransactionInfo.nYesVotes++;

                if (currentTransactionInfo.everyoneVotedYes()) {
                    fixAndCommunicateDecisionSimulatingCrash(msg.transactionId, Decision.COMMIT);
                    System.out.println("COORDINATOR " + id + " COMMIT OK");
                }
            } else {
                fixAndCommunicateDecisionSimulatingCrash(msg.transactionId, Decision.ABORT);
                System.out.println("COORDINATOR " + id + " COMMIT FAIL");
            }
        }
    }

    @Override
    public void onRecovery(Recovery msg) {
        getContext().become(createReceive());

        for (Map.Entry<String, TransactionInfo> ongoingTransaction: ongoingTransactions.entrySet()){
            if (ongoingTransaction.getValue().isTransactionEnded) {
                String transactionId = ongoingTransaction.getKey();
                if(!hasDecided(transactionId)) {
                    fixDecision(transactionId, Decision.ABORT);
                    sendMessageToClient(ongoingTransaction.getValue().client, new Client.TxnResultMsg(false));
                }

                Decision decision = decisions.get(transactionId);
                sendMessageCorrectlyToAllContactedServers(new DecisionResponse(transactionId, decision));
                ongoingTransactions.remove(transactionId);
            }
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

    private void sendMessageCorrectlyToAllContactedServers(Message msg) {
        TransactionInfo currentTransactionInfo = ongoingTransactions.get(msg.transactionId);
        for(ActorRef server : currentTransactionInfo.contactedServers) {
            server.tell(msg, getSelf());
        }
    }

    private void sendMessageToFirstContactedServerAndThenCrash(Message msg, int recoverIn) {
        TransactionInfo currentTransactionInfo = ongoingTransactions.get(msg.transactionId);
        for(ActorRef server : currentTransactionInfo.contactedServers) {
            server.tell(msg, getSelf());
            crash(recoverIn);
            return;
        }
    }

    private void fixAndCommunicateDecisionSimulatingCrash(String transactionId, Decision decision) {
        TransactionInfo currentTransactionInfo = ongoingTransactions.get(transactionId);

        fixDecision(transactionId, decision);
        sendMessageToClient(currentTransactionInfo.client, new Client.TxnResultMsg(decision == Decision.COMMIT));
        sendMessageToAllContactedServersSimulatingCrash(
                new DecisionResponse(transactionId, decision),
                CRASH_DURING_SEND_DECISION
        );
    }



    private void sendMessageToAllContactedServersSimulatingCrash(Message msg, CrashType crashType) {
        switch (crashType) {
            case NONE:
                sendMessageCorrectlyToAllContactedServers(msg);
                ongoingTransactions.remove(msg.transactionId);
                break;
            case AFTER_FIRST_SEND:
                sendMessageToFirstContactedServerAndThenCrash(msg, 3000);
                break;
            case AFTER_ALL_SENDS:
                sendMessageCorrectlyToAllContactedServers(msg);
                crash(5000);
                break;
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
                .build();
    }
}
