package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.List;
import java.util.ArrayList;

import java.io.IOException;

public class Main {
  final static int N_CLIENTS = 3;
  final static int N_COORDINATORS = 5;
  final static int N_SERVERS = 10;
  final static int N_KEYS_PER_SERVER = 10;
  final static int MAX_KEY = N_SERVERS * N_KEYS_PER_SERVER - 1;


  /*-- Main ------------------------------------------------------------------*/
  public static void main(String[] args) {

    // Create the actor system
    final ActorSystem system = ActorSystem.create("akka_project");

    // Create actors
    List<ActorRef> clientsGroup = new ArrayList<>();
    for (int i = 0; i< N_CLIENTS; i++) {
      clientsGroup.add(system.actorOf(Client.props(i), "client" + i));
    }

    List<ActorRef> coordinatorsGroup = new ArrayList<>();
    for (int i = 0; i< N_COORDINATORS; i++) {
      coordinatorsGroup.add(system.actorOf(Coordinator.props(i), "coordinator" + i));
    }

    List<ActorRef> serversGroup = new ArrayList<>();
    for (int i = 0; i< N_SERVERS; i++) {
      serversGroup.add(system.actorOf(Server.props(i), "server" + i));
    }

    // Send start messages to the clients to inform them of the MAX_KEY and coordinatorsGroup
    Client.WelcomeMsg clientWelcomeMsg = new Client.WelcomeMsg(MAX_KEY, coordinatorsGroup);
    for (ActorRef client: clientsGroup) {
      client.tell(clientWelcomeMsg, null);
    }

    // Send start messages to the coordinators to inform them of the serversGroup
    Coordinator.WelcomeMsg coordinatorWelcomeMsg = new Coordinator.WelcomeMsg(serversGroup);
    for (ActorRef coordinator: coordinatorsGroup) {
      coordinator.tell(coordinatorWelcomeMsg, null);
    }

    try {
      System.out.println(">>> Press ENTER to exit <<<");
      System.in.read();
    } 
    catch (IOException ignored) {}

    Client.StopMsg stopMsg = new Client.StopMsg();
    for (ActorRef server: clientsGroup) {
      server.tell(stopMsg, null);
    }

    // wait for all messages to flush
    try { Thread.sleep(2000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    // send requests to servers to log the sum of their values
    Server.LogRequestMsg logRequestMsg = new Server.LogRequestMsg();
    for (ActorRef server: serversGroup) {
      server.tell(logRequestMsg, null);
    }

    // wait for all servers to log the sum of their values
    try { Thread.sleep(2000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    system.terminate();
  }
}
