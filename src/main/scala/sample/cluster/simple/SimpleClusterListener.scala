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

  var dataMap: mutable.HashMap[String, String] = mutable.HashMap[String, String]()


  // subscribe to cluster changes, re-subscribe when restart
  override def preStart(): Unit = {
    if (cluster.state.members.count(_.status == MemberStatus.up) == 1) {
      initNode = true
    }

    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = cluster.unsubscribe(self)



  def findSuccessorNode(key : Object): Address = {


    val keyPos = key.hashCode % 1000000
    var min = 1000000
    var minAddr: Address = null

    for (member <- cluster.state.members) {
      if (!member.hasRole("frontendapi") && member.address.port.hashCode % 1000000 >= keyPos && member.address.port.hashCode % 1000000 < min) {
        min = member.address.port.hashCode % 1000000
        minAddr = member.address
      }
    }
    // If null -> find the lowest
    if (minAddr == null) {
      println("nullem jestem")
      var min = 1000000

      for (member <- cluster.state.members) {
        if (member.address.port.hashCode % 1000000 <= min && !member.hasRole("frontendapi"))
          minAddr = member.address
      }
    }
    minAddr



    //TODO There is an error to be found - error when 'for' finishes without returning a value
/*
    for (node <- nodes) {
      if (node._1 % 1000000 > key.hashCode() % 1000000)
        return node._2.address
    }
    return nodes.head._2.address.
      */
  }


  // returns true if the current node is the successor of the node with a given address
  def harryYouAreTheChosenOne(addr: Address): Boolean = {
    currentMember.get.address == findSuccessorNode(addr)
  }


  def findPredecessorNode(key: Object): Address = {
    var addr = nodes.last._2.address
    for (node <- nodes) {
      if (node._2.address.port.hashCode % 1000000 > key.hashCode % 1000000)
        return addr
      addr = node._2.address
    }
    nodes.last._2.address
  }



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
            nodes = nodes.+((member.address.port.hashCode, member))
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
      nodes = nodes.-(member.address.port.hashCode)

      if (harryYouAreTheChosenOne(member.address)) {

        // copy data for which the removed member was the primary node (copy to the successor or current node)
        // if current node is 3 and the removed one was 2
        //    you need to know the hash of node 2 and the data from node 3 (current node)
        //    you copy the data to node 4
        val hashPos = member.address.port.hashCode % 1000000
        var pack : mutable.Map[String, String] = mutable.Map[String, String]()
        for ((key, value) <- dataMap) {
          if (key.hashCode % 1000000 <= hashPos ||
             (key.hashCode % 1000000 > hashPos) && key.hashCode % 1000000 > nodes.last._2.address.port.hashCode % 1000000) {
            pack.+((key, value))
          }
        }
        context.actorSelection(RootActorPath(findSuccessorNode(currentMember.get.address)) / "user" / "clusterListener") ! DataPackage(pack)

        // copy data for which the removed member was the backup node (copy to the current node)
        // if current node is 3 and the removed one was 2
        //    you need to know the hash of node 0 and the data from node 1
        //    you copy the data to node 3 (current node)

        // sending request to the predecessor of the removed node (removed node is no longer in 'nodes')
        context.actorSelection(RootActorPath(findPredecessorNode(currentMember.get.address)) / "user" / "clusterListener") ! DataPackageRequest()
      }


    case DataPackage(pack : mutable.Map[String, String]) =>
      for ((key, value) <- pack) {
        dataMap.put(key, value)
      }


    // to find the data for which the removed node was the backup node we have to find all data for which the predecessor of the removed node is the primary node
    case DataPackageRequest() =>
      var map : mutable.Map[String, String] = mutable.Map[String, String]()
      for ((key, value) <- dataMap) {
        if (findSuccessorNode(key) == currentMember.get.address) {
          map.+((key, value))
        }
      }
      sender ! DataPackage(map)


    case Add(key: String, value: String) =>
      println("dodawanie " + key + " " + value)

      val minAddr = findSuccessorNode(key)
      val nextMinAddr = findSuccessorNode(minAddr)

      context.actorSelection(RootActorPath(minAddr) / "user" / "clusterListener") ! RealAdd(key, value)

      if (minAddr != nextMinAddr)
        context.actorSelection(RootActorPath(nextMinAddr) / "user" / "clusterListener") ! RealAdd(key, value)


    case RealAdd(key: String, value: String) =>
      println("real add: " + key + " " + value)
      dataMap.put(key, value)
      for ((key, value) <- dataMap)
        printf(key + " " + value + "\n")


    // TODO
    case GetOne(key) =>
      sender ! "getOne here"
      context.actorSelection(RootActorPath(findSuccessorNode(key)) / "user" / "clusterListener") ! RealGetOne(key, sender)

    case RealGetOne(key, node) =>
     for ((key, value) <- dataMap)
       printf(key + " " + value + "\n")
     dataMap.get(key) match {
       case Some(value) =>
         printf("Sending value to the frontend node: " + value)
         context.actorSelection(RootActorPath(node.path.address) / "user" / "frontend-api") ! Result(Some(value))
       case _ =>
         context.actorSelection(RootActorPath(node.path.address) / "user" / "frontend-api") ! Result(None)
         printf("No such key in this node")
     }


    //TODO
    case GetQuorum(key) =>
    //TODO


    case _: MemberEvent => // ignore
  }
}