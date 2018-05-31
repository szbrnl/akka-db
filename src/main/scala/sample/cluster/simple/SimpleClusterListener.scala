package sample.cluster.simple

import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.actor.{Actor, ActorLogging, ActorRef, RootActorPath}
import akka.cluster.ddata.ORMap
import akka.cluster.ddata.ORMultiMap
import akka.cluster.pubsub.DistributedPubSub
import sample.cluster.transformation.BackendRegistration


class SimpleClusterListener extends Actor with ActorLogging {

  val cluster = Cluster(context.system)
  val mediator: ActorRef = DistributedPubSub(context.system).mediator
  var dataMap = ORMultiMap.empty[String, String]

  // subscribe to cluster changes, re-subscribe when restart
  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive = {
    case msg : String =>
      log.info("hello darkness my old friend")

    case MemberUp(member) =>
      log.info("Member is Up: {}", member.address)
      if(member.hasRole("frontendapi")) {
        context.actorSelection(RootActorPath(member.address) / "user" / "frontend-api") !
          BackendRegistration

      }

      for (i <- cluster.state.members) {
        println("asdasdasd")
        if(!i.hasRole("frontendapi"))
            context.actorSelection(RootActorPath(member.address) / "user" / "clusterListener") ! "asdasdasd"
      }

      println("mam gooooooooo")

    case UnreachableMember(member) =>

      println(dataMap.getEntries())

      log.info("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}",
        member.address, previousStatus)

    case Add(key, value) =>
      println("dodawanie " + key + " " + value)
      // TODO
    case GetOne(key) =>
      //TODO
    case GetQuorum(key) =>
      //TODO



    case _: MemberEvent => // ignore
  }
}