package sample.cluster.simple

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, RootActorPath}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.cluster.pubsub.DistributedPubSub
import com.typesafe.config.ConfigException.Null
import com.typesafe.config.ConfigFactory
import sample.cluster.simple.FrontendActor._frontend
import sample.cluster.transformation.BackendRegistration


object SimpleClusterListener
{
  def initiate(_port : String): Unit = {
    var port = _port
    if (port == "") port = "0"

    val config = ConfigFactory.parseString(
      s"""
        akka.remote.netty.tcp.port=$port
        akka.remote.artery.canonical.port=$port
        """)
      .withFallback(ConfigFactory.parseString("akka.cluster.roles = [backend]"))
      .withFallback(ConfigFactory.load())

    val system = ActorSystem("ClusterSystem", config)
    // Create an actor that handles cluster domain events
    system.actorOf(Props[SimpleClusterListener], name = "backend-api")
  }
}


class SimpleClusterListener extends Actor with ActorLogging{


  val cluster = Cluster(context.system)
  val mediator: ActorRef = DistributedPubSub(context.system).mediator
/*
  var position = 0
  setPosition(cluster)

  def setPosition(cluster : Cluster) = {
    var backends = 1
    for (member <- cluster.state.members) {
      if (!member.hasRole("frontendapi")) backends += 1
    }
    for (i <- 0 to 20) {
      val num = 2 ^ i
      for (j <- 0 to num) {
        val pos = (1000000 / num) * j
        var flag = 0
        for (member <- cluster.state.members) {
          if (!member.hasRole("frontendapi"))
            context.actorSelection(RootActorPath(member.address) / "user" / "clusterListener") ! GetPosition()
          def receive = {
            case p: Int =>
              if (p == pos) flag = 1
          }
        }
        if (flag == 0) position = pos
      }
    }
  }

  def getPosition() : Int = {
    return position
  }

*/



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
        println("test receive'a\n")

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
/*
    case GetPosition() =>
      println("Position request")
      sender ! getPosition()
*/
    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}",
        member.address, previousStatus)

    case Add(key : String, value : String) =>

      println("dodawanie " + key + " " + value)

      // TODO
    case GetOne(key) =>
      //TODO
    case GetQuorum(key) =>
      //TODO



    case _: MemberEvent => // ignore
  }
}