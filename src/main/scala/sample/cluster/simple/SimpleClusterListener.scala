package sample.cluster.simple

import akka.actor.{Actor, ActorLogging, ActorRef, Address, RootActorPath}
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.cluster.ClusterEvent._
import akka.cluster.pubsub.DistributedPubSub
import com.typesafe.config.ConfigException.Null
import sample.cluster.transformation.BackendRegistration

import scala.collection.mutable


class SimpleClusterListener extends Actor with ActorLogging {


  val cluster = Cluster(context.system)
  val mediator: ActorRef = DistributedPubSub(context.system).mediator

  var nodes: mutable.SortedMap[Int, Member] = mutable.SortedMap[Int, Member]()
  var currentMember: Option[Member] = None

  var initNode: Boolean = false

  // subscribe to cluster changes, re-subscribe when restart
  override def preStart(): Unit = {
    if (cluster.state.members.count(_.status == MemberStatus.up) == 1) {
      initNode = true
    }

    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive = {
    case msg: String =>
      log.info(msg)
      println(currentMember.get.address)


    case MemberUp(member) =>
      log.info("Member is Up: {}", member.address)

      if (member.hasRole("frontendapi")) {
        context.actorSelection(RootActorPath(member.address) / "user" / "frontend-api") !
          BackendRegistration
      }
      else {
        currentMember match {
          case None =>
            if (initNode) {
              log.info("welcome received {}", member.address)
              currentMember = Some(member)
            }
          case _ =>
            context.actorSelection(RootActorPath(member.address) / "user" / "clusterListener") ! WelcomeMessage(member)
            nodes = nodes.+((member.address.port.hashCode(), member))
        }
      }


    case WelcomeMessage(member) =>
      currentMember match {
        case None =>
          log.info("welcome1 received {}", member.address)
          currentMember = Some(member)
        case _ => // Ignore

      }

    case UnreachableMember(member) =>
      log.info("Member detected as unreachable: {}", member)


    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}",
        member.address, previousStatus)
      nodes = nodes.-(member.address.hashCode)


    case Add(key: String, value: String) =>
      println("dodawanie " + key + " " + value)

      val keyPos = key.hashCode % 1000000
      var min = 1000000
      var minAddr: Address = null


      // TODO refactor
      for (member <- cluster.state.members) {
        if (!member.hasRole("frontendapi") && member.address.hashCode % 1000000 >= keyPos && member.address.hashCode % 1000000 < min) {
          min = member.address.hashCode
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
      context.actorSelection(RootActorPath(minAddr) / "user" / "clusterListener") ! RealAdd(key, value)


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