package it.unitn.ds1;

import akka.actor.*;

import java.io.Serializable;
import java.util.*;

public class Coordinator extends AbstractActor{

    private final Integer coordinatorId;
    private List<ActorRef> servers;
    private final Map<ActorRef, TransactionInfo> ongoingTransactions = new HashMap<>();

    public Coordinator(int coordinatorId) {
        this.coordinatorId = coordinatorId;
    }

    static public Props props(int coordinatorId) {
        return Props.create(Coordinator.class, () -> new Coordinator(coordinatorId));
    }

    public static class TransactionInfo implements Serializable {
        public final Set<ActorRef> servers;
        public Integer nYesVotes;
        public TransactionInfo() {
            this.servers = new HashSet<>();
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
        public final ActorRef client;
        public final Integer key; // the key of the value to read
        public ReadMsg(ActorRef client, int key) {
            this.client = client;
            this.key = key;
        }
    }

    // reply from the server when requested a READ on a given key
    public static class ReadResultMsg implements Serializable {
        public final ActorRef client; // the client to which forward the result
        public final Integer key; // the key associated to the requested item
        public final Integer value; // the value found in the data store for that item
        public ReadResultMsg(ActorRef client, int key, int value) {
            this.client = client;
            this.key = key;
            this.value = value;
        }
    }

    // WRITE request from the coordinator to the server
    public static class WriteMsg implements Serializable {
        public final ActorRef client;
        public final Integer key; // the key of the value to write
        public final Integer value; // the new value to write
        public WriteMsg(ActorRef client, int key, int value) {
            this.client = client;
            this.key = key;
            this.value = value;
        }
    }

    public enum Vote {NO, YES}
    public enum Decision {ABORT, COMMIT}

    public static class VoteRequest implements Serializable {
        public final ActorRef client;
        public VoteRequest(ActorRef client) {
            this.client = client;
        }
    }

    public static class VoteResponse implements Serializable {
        public final ActorRef client;
        public final Vote vote;
        public VoteResponse(ActorRef client, Vote v) {
            this.client = client;
            vote = v;
        }
    }

    public static class DecisionMsg implements Serializable {
        public final ActorRef client;
        public final Decision decision;
        public DecisionMsg(ActorRef client, Decision d) {
            this.client = client;
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
        ensureContactedServersExists(currentClient);
    }

    private void ensureContactedServersExists(ActorRef client) {
        if (!ongoingTransactions.containsKey(client)) {
            ongoingTransactions.put(client, new TransactionInfo());
        }
    }

    private void addContactedServer(ActorRef client, ActorRef server) {
        ongoingTransactions.get(client).servers.add(server);
    }

    private void onReadMsg(Client.ReadMsg msg) {
        ActorRef currentClient = getSender();
        ActorRef currentServer = getServerFromKey(msg.key);
        currentServer.tell(new ReadMsg(getSender(), msg.key), getSelf());
        addContactedServer(currentClient, currentServer);
    }

    private void onReadResultMsg(ReadResultMsg msg) {
        msg.client.tell(new Client.ReadResultMsg(msg.key, msg.value), getSelf());

        System.out.println("COORDINATOR " + coordinatorId + " SEND READ RESULT (" + msg.key + ", " + msg.value + ") TO " + msg.client.path().name());
    }

    private void onWriteMsg(Client.WriteMsg msg) {
        ActorRef currentClient = getSender();
        ActorRef currentServer = getServerFromKey(msg.key);
        currentServer.tell(new WriteMsg(currentClient, msg.key, msg.value), getSelf());
        addContactedServer(currentClient, currentServer);

        System.out.println("COORDINATOR " + coordinatorId + " WRITE (" + msg.key + ", " + msg.value + ")");
    }

    private void onTxnEndMsg(Client.TxnEndMsg msg) {
        ActorRef currentClient = getSender();

        if (msg.commit) {
            Set<ActorRef> contactedServers = ongoingTransactions.get(currentClient).servers;
            for(ActorRef server : contactedServers) {
                server.tell(new VoteRequest(currentClient), getSelf());
            }
        } else {
            currentClient.tell(new Client.TxnResultMsg(false), getSelf());
            sendDecisionToAllContactedServers(currentClient, Decision.ABORT);
            ongoingTransactions.remove(currentClient);
        }

        System.out.println("COORDINATOR " + coordinatorId + " END");
    }

    private void onVoteResponseMsg(VoteResponse msg) {
        TransactionInfo currentTransactionInfo = ongoingTransactions.get(msg.client);

        if (currentTransactionInfo != null) {
            if (msg.vote == Vote.YES) {
                currentTransactionInfo.nYesVotes++;

                if (currentTransactionInfo.nYesVotes == currentTransactionInfo.servers.size()) {
                    msg.client.tell(new Client.TxnResultMsg(true), getSelf());
                    sendDecisionToAllContactedServers(msg.client, Decision.COMMIT);
                    System.out.println("COORDINATOR " + coordinatorId + " COMMIT OK");
                    ongoingTransactions.remove(msg.client);
                }
            } else {
                msg.client.tell(new Client.TxnResultMsg(false), getSelf());
                sendDecisionToAllContactedServers(msg.client, Decision.ABORT);
                System.out.println("COORDINATOR " + coordinatorId + " COMMIT FAIL");
                ongoingTransactions.remove(msg.client);
            }
        }
    }

    private void sendDecisionToAllContactedServers(ActorRef client, Decision decision) {
        TransactionInfo currentTransactionInfo = ongoingTransactions.get(client);
        for(ActorRef server : currentTransactionInfo.servers) {
            server.tell(new DecisionMsg(client, decision), getSelf());
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
