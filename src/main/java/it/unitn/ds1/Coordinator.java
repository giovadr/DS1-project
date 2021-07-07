package it.unitn.ds1;

import akka.actor.*;

import java.io.Serializable;
import java.util.*;

public class Coordinator extends AbstractActor{

    private final Integer coordinatorId;
    private Integer transactionsCounter;
    private List<ActorRef> servers;
    private final Map<String, TransactionInfo> ongoingTransactions = new HashMap<>();
    private final Map<ActorRef, String> transactionIdForClients = new HashMap<>();

    public Coordinator(int coordinatorId) {
        this.coordinatorId = coordinatorId;
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

    public static class WelcomeMsg implements Serializable {
        public final List<ActorRef> servers;
        public WelcomeMsg(List<ActorRef> servers) {
            this.servers = Collections.unmodifiableList(new ArrayList<>(servers));
        }
    }

    // READ request from the coordinator to the server
    public static class ReadMsg implements Serializable {
        public final String transactionId;
        public final Integer key; // the key of the value to read
        public ReadMsg(String transactionId, int key) {
            this.transactionId = transactionId;
            this.key = key;
        }
    }

    // reply from the server when requested a READ on a given key
    public static class ReadResultMsg implements Serializable {
        public final String transactionId;
        public final Integer key; // the key associated to the requested item
        public final Integer value; // the value found in the data store for that item
        public ReadResultMsg(String transactionId, int key, int value) {
            this.transactionId = transactionId;
            this.key = key;
            this.value = value;
        }
    }

    // WRITE request from the coordinator to the server
    public static class WriteMsg implements Serializable {
        public final String transactionId;
        public final Integer key; // the key of the value to write
        public final Integer value; // the new value to write
        public WriteMsg(String transactionId, int key, int value) {
            this.transactionId = transactionId;
            this.key = key;
            this.value = value;
        }
    }

    public enum Vote {NO, YES}
    public enum Decision {ABORT, COMMIT}

    public static class VoteRequest implements Serializable {
        public final String transactionId;
        public VoteRequest(String transactionId) {
            this.transactionId = transactionId;
        }
    }

    public static class VoteResponse implements Serializable {
        public final String transactionId;
        public final Vote vote;
        public VoteResponse(String transactionId, Vote v) {
            this.transactionId = transactionId;
            vote = v;
        }
    }

    public static class DecisionMsg implements Serializable {
        public final String transactionId;
        public final Decision decision;
        public DecisionMsg(String transactionId, Decision d) {
            this.transactionId = transactionId;
            decision = d;
        }
    }

    private void onWelcomeMsg(WelcomeMsg msg) {
        this.servers = msg.servers;
        System.out.println(servers);
    }

    private void onTxnBeginMsg(Client.TxnBeginMsg msg) {
        ActorRef currentClient = getSender();
        currentClient.tell(new Client.TxnAcceptMsg(), getSelf());
        initializeTransaction(currentClient);
    }

    private void initializeTransaction(ActorRef client) {
        String transactionId = coordinatorId + "." + transactionsCounter;
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

        System.out.println("COORDINATOR " + coordinatorId + " SEND READ RESULT (" + msg.key + ", " + msg.value + ") TO " + currentClient.path().name());
    }

    private void onWriteMsg(Client.WriteMsg msg) {
        ActorRef currentClient = getSender();
        String transactionId = transactionIdForClients.get(currentClient);
        ActorRef currentServer = getServerFromKey(msg.key);

        currentServer.tell(new WriteMsg(transactionId, msg.key, msg.value), getSelf());
        addContactedServer(transactionId, currentServer);

        System.out.println("COORDINATOR " + coordinatorId + " WRITE (" + msg.key + ", " + msg.value + ")");
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

        System.out.println("COORDINATOR " + coordinatorId + " END");
    }

    private void onVoteResponseMsg(VoteResponse msg) {
        TransactionInfo currentTransactionInfo = ongoingTransactions.get(msg.transactionId);

        if (currentTransactionInfo != null) {
            if (msg.vote == Vote.YES) {
                currentTransactionInfo.nYesVotes++;

                if (currentTransactionInfo.nYesVotes == currentTransactionInfo.contactedServers.size()) {
                    currentTransactionInfo.client.tell(new Client.TxnResultMsg(true), getSelf());
                    sendDecisionToAllContactedServers(msg.transactionId, Decision.COMMIT);
                    System.out.println("COORDINATOR " + coordinatorId + " COMMIT OK");
                    ongoingTransactions.remove(msg.transactionId);
                }
            } else {
                currentTransactionInfo.client.tell(new Client.TxnResultMsg(false), getSelf());
                sendDecisionToAllContactedServers(msg.transactionId, Decision.ABORT);
                System.out.println("COORDINATOR " + coordinatorId + " COMMIT FAIL");
                ongoingTransactions.remove(msg.transactionId);
            }
        }
    }

    private void sendDecisionToAllContactedServers(String transactionId, Decision decision) {
        TransactionInfo currentTransactionInfo = ongoingTransactions.get(transactionId);
        for(ActorRef server : currentTransactionInfo.contactedServers) {
            server.tell(new DecisionMsg(transactionId, decision), getSelf());
        }
    }

    private ActorRef getServerFromKey(int key) {
        int serverId = key / 10;
        return servers.get(serverId);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(WelcomeMsg.class,  this::onWelcomeMsg)
                .match(Client.TxnBeginMsg.class,  this::onTxnBeginMsg)
                .match(Client.ReadMsg.class,  this::onReadMsg)
                .match(ReadResultMsg.class,  this::onReadResultMsg)
                .match(Client.WriteMsg.class,  this::onWriteMsg)
                .match(Client.TxnEndMsg.class,  this::onTxnEndMsg)
                .match(VoteResponse.class,  this::onVoteResponseMsg)
                .build();
    }
}
