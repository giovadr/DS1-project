package it.unitn.ds1;

import akka.actor.*;

import java.io.Serializable;
import java.util.*;

public class Coordinator extends Node {
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

    public static class TransactionInfo implements Serializable {
        public final ActorRef client;
        public final Set<ActorRef> contactedServers;
        public Integer nYesVotes;
        public TransactionInfo(ActorRef client) {
            this.client = client;
            this.contactedServers = new HashSet<>();
            this.nYesVotes = 0;
        }
    }

    private void onTxnBeginMsg(Client.TxnBeginMsg msg) {
        ActorRef currentClient = getSender();
        currentClient.tell(new Client.TxnAcceptMsg(), getSelf());
        initializeTransaction(currentClient);
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

    private void onReadMsg(Client.ReadMsg msg) {
        ActorRef currentClient = getSender();
        String transactionId = transactionIdForClients.get(currentClient);
        ActorRef currentServer = getServerFromKey(msg.key);
        currentServer.tell(new ReadMsg(transactionId, msg.key), getSelf());
        addContactedServer(transactionId, currentServer);
    }

    private void onReadResultMsg(ReadResultMsg msg) {
        ActorRef currentClient = ongoingTransactions.get(msg.transactionId).client;
        currentClient.tell(new Client.ReadResultMsg(msg.key, msg.value), getSelf());

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

        if (msg.commit) {
            Set<ActorRef> contactedServers = ongoingTransactions.get(transactionId).contactedServers;
            for(ActorRef server : contactedServers) {
                server.tell(new VoteRequest(transactionId), getSelf());
            }
        } else {
            currentClient.tell(new Client.TxnResultMsg(false), getSelf());
            sendDecisionToAllContactedServers(transactionId, Decision.ABORT);
            ongoingTransactions.remove(transactionId);
        }

        System.out.println("COORDINATOR " + id + " END");
    }

    private void onVoteResponseMsg(VoteResponse msg) {
        TransactionInfo currentTransactionInfo = ongoingTransactions.get(msg.transactionId);

        if (currentTransactionInfo != null) {
            if (msg.vote == Vote.YES) {
                currentTransactionInfo.nYesVotes++;

                if (currentTransactionInfo.nYesVotes == currentTransactionInfo.contactedServers.size()) {
                    currentTransactionInfo.client.tell(new Client.TxnResultMsg(true), getSelf());
                    sendDecisionToAllContactedServers(msg.transactionId, Decision.COMMIT);
                    System.out.println("COORDINATOR " + id + " COMMIT OK");
                    ongoingTransactions.remove(msg.transactionId);
                }
            } else {
                currentTransactionInfo.client.tell(new Client.TxnResultMsg(false), getSelf());
                sendDecisionToAllContactedServers(msg.transactionId, Decision.ABORT);
                System.out.println("COORDINATOR " + id + " COMMIT FAIL");
                ongoingTransactions.remove(msg.transactionId);
            }
        }
    }

    private void sendDecisionToAllContactedServers(String transactionId, Decision decision) {
        TransactionInfo currentTransactionInfo = ongoingTransactions.get(transactionId);
        for(ActorRef server : currentTransactionInfo.contactedServers) {
            server.tell(new DecisionResponse(transactionId, decision), getSelf());
        }
    }

    private ActorRef getServerFromKey(int key) {
        int serverId = key / 10;
        return servers.get(serverId);
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
