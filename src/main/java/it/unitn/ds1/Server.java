package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Server extends AbstractActor {

    private final Integer serverId;

    public Server(int serverId) {
        this.serverId = serverId;
    }

    static public Props props(int serverId) {
        return Props.create(Server.class, () -> new Server(serverId));
    }

    @Override
    public AbstractActor.Receive createReceive() {
        return null;
    }
}
