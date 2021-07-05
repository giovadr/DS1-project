package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.*;

public class Server extends AbstractActor {

    private final Integer serverId;
    private final DataEntry[] globalWorkspace = new DataEntry[Main.N_KEYS_PER_SERVER];
    private final Map<ActorRef, DataEntry[]> localWorkspaces = new HashMap<>();

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
    }

    public Server(int serverId) {
        this.serverId = serverId;
        for (int i = 0; i < Main.N_KEYS_PER_SERVER; i++) {
            globalWorkspace[i] = new DataEntry(0, 100);
        }
    }

    static public Props props(int serverId) {
        return Props.create(Server.class, () -> new Server(serverId));
    }

    public static class LogRequestMsg implements Serializable {}

    private void onReadMsg(Coordinator.ReadMsg msg) {
        ensureLocalWorkspaceExists(msg.client);

        DataEntry[] localWorkspace = localWorkspaces.get(msg.client);
        Coordinator.ReadResultMsg readResult = new Coordinator.ReadResultMsg(msg.client, msg.key, localWorkspace[msg.key % 10].value);
        localWorkspace[msg.key % 10].readsCounter++;

        ActorRef currentCoordinator = getSender();
        currentCoordinator.tell(readResult, getSelf());

        System.out.println("SERVER " + serverId + " SEND READ RESULT (" + readResult.key + ", " + readResult.value + ") TO COORDINATOR");
    }

    private void onWriteMsg(Coordinator.WriteMsg msg) {
        ensureLocalWorkspaceExists(msg.client);

        DataEntry[] localWorkspace = localWorkspaces.get(msg.client);
        localWorkspace[msg.key % 10].value = msg.value;
        localWorkspace[msg.key % 10].writesCounter++;

        System.out.println("SERVER " + serverId + " WRITE (" + msg.key + ", " + localWorkspace[msg.key % 10].value + ")");
    }

    private void onVoteRequestMsg(Coordinator.VoteRequest msg) {
        DataEntry[] localWorkspace = localWorkspaces.get(msg.client);
        boolean canCommit = true;

        for (int i = 0; i < Main.N_KEYS_PER_SERVER; i++) {
            // check that versions are the same, only if there were reads or writes
            if ((localWorkspace[i].readsCounter > 0 || localWorkspace[i].writesCounter > 0) && !localWorkspace[i].version.equals(globalWorkspace[i].version)) {
                System.out.println("SERVER " + serverId + " VERSIONS ARE DIFFERENT");
                canCommit = false;
                break;
            }
            // if there is a write lock, we cannot read or write
            if (globalWorkspace[i].writesCounter > 0 && (localWorkspace[i].readsCounter > 0 || localWorkspace[i].writesCounter > 0)) {
                System.out.println("SERVER " + serverId + " WRITE LOCK (" + globalWorkspace[i].writesCounter + "), " +
                        "CANNOT READ (" + localWorkspace[i].readsCounter + ") OR WRITE(" + localWorkspace[i].writesCounter + ")");
                canCommit = false;
                break;
            }
            // if there is a read lock, we cannot write
            if (globalWorkspace[i].readsCounter > 0 && localWorkspace[i].writesCounter > 0) {
                System.out.println("SERVER " + serverId + " READ LOCK, CANNOT WRITE");
                canCommit = false;
                break;
            }
        }

        System.out.println("SERVER " + serverId + " CAN COMMIT? " + canCommit);

        // update locks in the global workspace
        if (canCommit) {
            for (int i = 0; i < Main.N_KEYS_PER_SERVER; i++) {
                globalWorkspace[i].readsCounter += localWorkspace[i].readsCounter;
                globalWorkspace[i].writesCounter += localWorkspace[i].writesCounter;
            }
        }

        Coordinator.Vote vote;
        if (canCommit) {
            vote = Coordinator.Vote.YES;
        } else {
            vote = Coordinator.Vote.NO;
        }

        ActorRef currentCoordinator = getSender();
        currentCoordinator.tell(new Coordinator.VoteResponse(msg.client, vote), getSelf());

        System.out.println("SERVER " + serverId + " VOTE " + vote);
    }

    private void onDecisionMsg(Coordinator.DecisionMsg msg) {
        DataEntry[] localWorkspace = localWorkspaces.get(msg.client);

        //remove locks in the global workspace
        for (int i = 0; i < Main.N_KEYS_PER_SERVER; i++) {
            globalWorkspace[i].readsCounter -= localWorkspace[i].readsCounter;
            globalWorkspace[i].writesCounter -= localWorkspace[i].writesCounter;
        }

        if (msg.decision == Coordinator.Decision.COMMIT) {
            // update values that were written
            for (int i = 0; i < Main.N_KEYS_PER_SERVER; i++) {
                if(localWorkspace[i].writesCounter > 0) {
                    globalWorkspace[i].version++;
                    globalWorkspace[i].value = localWorkspace[i].value;
                }
            }
        }

        localWorkspaces.remove(msg.client);

        System.out.println("SERVER " + serverId + " FINAL DECISION " + msg.decision);
    }

    private void onLogRequestMsg(LogRequestMsg msg) {
        int sum = 0;
        for (int i = 0; i < Main.N_KEYS_PER_SERVER; i++) {
            sum += globalWorkspace[i].value;
        }
        System.out.println("SERVER " + serverId + " FINAL SUM " + sum);
    }

    private void ensureLocalWorkspaceExists(ActorRef client) {
        if (!localWorkspaces.containsKey(client)) {
            DataEntry[] localWorkspace = new DataEntry[Main.N_KEYS_PER_SERVER];
            for (int i = 0; i < Main.N_KEYS_PER_SERVER; i++) {
                localWorkspace[i] = new DataEntry(globalWorkspace[i].version, globalWorkspace[i].value);
            }
            localWorkspaces.put(client, localWorkspace);
        }
    }

    @Override
    public AbstractActor.Receive createReceive() {
        return receiveBuilder()
                .match(Coordinator.ReadMsg.class,  this::onReadMsg)
                .match(Coordinator.WriteMsg.class, this::onWriteMsg)
                .match(Coordinator.VoteRequest.class, this::onVoteRequestMsg)
                .match(Coordinator.DecisionMsg.class, this::onDecisionMsg)
                .match(LogRequestMsg.class, this::onLogRequestMsg)
                .build();
    }
}
