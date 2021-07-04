package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.*;

public class Server extends AbstractActor {

    private final Integer serverId;
    private final DataEntry[] data = new DataEntry[Main.N_KEYS_PER_SERVER];
    private final Map<ActorRef, DataEntry[]> localWorkspaces = new HashMap<>();

    public static class DataEntry {
        public final Integer version;
        public final Integer value;
        public DataEntry(int version, int value) {
            this.version = version;
            this.value = value;
        }
    }

    public Server(int serverId) {
        this.serverId = serverId;
        Arrays.fill(data, new DataEntry(0, 100));
    }

    static public Props props(int serverId) {
        return Props.create(Server.class, () -> new Server(serverId));
    }

    private void onReadMsg(Coordinator.ReadMsg msg) {
        ensureLocalWorkspaceExists(msg.client);

        DataEntry[] localWorkspace = localWorkspaces.get(msg.client);
        Coordinator.ReadResultMsg readResult = new Coordinator.ReadResultMsg(msg.client, msg.key, localWorkspace[msg.key % 10].value);

        ActorRef currentCoordinator = getSender();
        currentCoordinator.tell(readResult, getSelf());

        System.out.println("SERVER " + serverId + " SEND READ RESULT (" + readResult.key + ", " + readResult.value + ") TO COORDINATOR");
    }

    private void onWriteMsg(Coordinator.WriteMsg msg) {
        ensureLocalWorkspaceExists(msg.client);

        DataEntry[] localWorkspace = localWorkspaces.get(msg.client);
        DataEntry previousDataEntry = localWorkspace[msg.key % 10];
        localWorkspace[msg.key % 10] = new DataEntry(previousDataEntry.version, msg.value);

        System.out.println("SERVER " + serverId + " WRITE (" + msg.key + ", " + localWorkspace[msg.key % 10].value + ")");
    }

    private void onVoteRequestMsg(Coordinator.VoteRequest msg) {
        DataEntry[] localWorkspace = localWorkspaces.get(msg.client);
        boolean localWorkspaceEqualsData = true;

        // TODO: implement locks
        // TODO: check only accessed values
        for (int i = 0; i < Main.N_KEYS_PER_SERVER; i++) {
            if (!localWorkspace[i].version.equals(data[i].version)) {
                localWorkspaceEqualsData = false;
                break;
            }
        }

        Coordinator.Vote vote;
        if (localWorkspaceEqualsData) {
            vote = Coordinator.Vote.YES;
        } else {
            vote = Coordinator.Vote.NO;
        }

        ActorRef currentCoordinator = getSender();
        currentCoordinator.tell(new Coordinator.VoteResponse(msg.client, vote), getSelf());
    }

    private void onDecisionMsg(Coordinator.DecisionMsg msg) {
        if (msg.decision == Coordinator.Decision.ABORT) {
            localWorkspaces.remove(msg.client);
        }
        // TODO: manage the case of Decision.COMMIT
    }

    private void ensureLocalWorkspaceExists(ActorRef client) {
        if (!localWorkspaces.containsKey(client)) {
            localWorkspaces.put(client, Arrays.copyOf(data, 10));
        }
    }

    @Override
    public AbstractActor.Receive createReceive() {
        return receiveBuilder()
                .match(Coordinator.ReadMsg.class,  this::onReadMsg)
                .match(Coordinator.WriteMsg.class, this::onWriteMsg)
                .match(Coordinator.VoteRequest.class, this::onVoteRequestMsg)
                .match(Coordinator.DecisionMsg.class, this::onDecisionMsg)
                .build();
    }
}
