package sample.cluster.simple

import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.actor.{Actor, ActorLogging, ActorRef, RootActorPath}
import akka.cluster.ddata.Replicator._
import akka.cluster.ddata._
import akka.cluster.pubsub.DistributedPubSub
import sample.cluster.transformation.BackendRegistration
import scala.concurrent.duration._
import akka.cluster.ddata.ORMultiMap



class SimpleClusterListener extends Actor with ActorLogging {

  val cluster = Cluster(context.system)
  val replicator = DistributedData(context.system).replicator
  val mediator: ActorRef = DistributedPubSub(context.system).mediator
  val dataMap = ORSetKey[(String, String)]("key")

  // subscribe to cluster changes, re-subscribe when restart
  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
  }

  replicator ! Subscribe(dataMap, self)

  override def postStop(): Unit = cluster.unsubscribe(self)
  var kkey = ""
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

 //     println(dataMap.getEntries())

      log.info("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}",
        member.address, previousStatus)

    case Add(key : String, value : String) =>
      val write = WriteTo(n = 1, timeout = 5.seconds)

      kkey = key
      val Counter1Key = PNCounterKey(key)
      replicator ! Update(dataMap, ORSet.empty[(String, String)],write)(_ + (key.toString, value.toString))
      println("dodawanie " + key + " " + value)

      val read = ReadLocal
      replicator ! Get(Counter1Key, read)

    case g @ GetSuccess(_, req) =>
      val value = g.get(PNCounterKey(kkey)).value
      println("jest tutaj" + value)
    case NotFound(_, req) => // key counter1 does not exist
      println("nie ma tutaj")

      // TODO
    case GetOne(key) =>
      //TODO
    case GetQuorum(key) =>
      //TODO



    case _: MemberEvent => // ignore
  }
}