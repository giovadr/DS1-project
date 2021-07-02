package it.unitn.ds1;

import akka.actor.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class Coordinator extends AbstractActor{

    private final Integer coordinatorId;

    public Coordinator(int coordinatorId) {
        this.coordinatorId = coordinatorId;
    }

    static public Props props(int coordinatorId) {
        return Props.create(Coordinator.class, () -> new Coordinator(coordinatorId));
    }

    public static class WelcomeMsg implements Serializable {
        public final List<ActorRef> servers;
        public WelcomeMsg(List<ActorRef> servers) {
            this.servers = Collections.unmodifiableList(new ArrayList<>(servers));
        }
    }

    @Override
    public Receive createReceive() {
        return null;
    }
}
