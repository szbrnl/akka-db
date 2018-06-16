package sample.cluster.simple

import akka.actor.{Actor, ActorLogging, ActorRef, Address, RootActorPath}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.cluster.pubsub.DistributedPubSub
import com.typesafe.config.ConfigException.Null
import sample.cluster.transformation.BackendRegistration


class SimpleClusterListener extends Actor with ActorLogging {


  val cluster = Cluster(context.system)
  val mediator: ActorRef = DistributedPubSub(context.system).mediator

  var selfAddress : Address = null


  // subscribe to cluster changes, re-subscribe when restart
  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])

  }


  override def postStop(): Unit = cluster.unsubscribe(self)



  def findNodeAddress(key : Object): Address = {
    val keyPos = key.hashCode % 1000000
    var min = 1000000
    var minAddr: Address = null

    for (member <- cluster.state.members) {
      if (!member.hasRole("frontendapi") && member.address.hashCode % 1000000 >= keyPos && member.address.hashCode % 1000000 < min) {
        min = member.address.hashCode % 1000000
        minAddr = member.address
      }
    }
    // If null -> find the lowest
    if (minAddr == null) {
      println("nullem jestem")
      var min = 1000000

      for (member <- cluster.state.members) {
        if (member.address.hashCode % 1000000 <= min)
          minAddr = member.address
      }
    }

    minAddr
  }


  // returns true if the current node is the successor of the node with a given address
  def harryYouAreTheChosenOne(addr: Address) = {

  }



  def receive = {
    case msg: String =>
      log.info(msg)

    case MemberUp(member) =>
      log.info("Member is Up: {}", member.address)
      if (member.hasRole("frontendapi")) {
        context.actorSelection(RootActorPath(member.address) / "user" / "frontend-api") !
          BackendRegistration
      }



    case UnreachableMember(member) =>
      log.info("Member detected as unreachable: {}", member)


    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}",
        member.address, previousStatus)


    case Add(key: String, value: String) =>
      println("dodawanie " + key + " " + value)

      val minAddr = findNodeAddress(key)
      val nextMinAddr = findNodeAddress(minAddr)

      context.actorSelection(RootActorPath(minAddr) / "user" / "clusterListener") ! RealAdd(key, value)

      if (minAddr != nextMinAddr)
        context.actorSelection(RootActorPath(nextMinAddr) / "user" / "clusterListener") ! RealAdd(key, value)


    case RealAdd(key: String, value: String) =>
      println("real add: " + key + " " + value)



    // TODO
    case GetOne(key) =>
    //TODO
    case GetQuorum(key) =>
    //TODO


    case _: MemberEvent => // ignore
  }
}