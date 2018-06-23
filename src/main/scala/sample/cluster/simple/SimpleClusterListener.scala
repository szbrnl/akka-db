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



  def findSuccessorNode(key : Object): Option[Address] = {
/*
//    printf("key: " + key + "\n")
    val keyPos = key.hashCode
    var min = Int.MaxValue
    var minAddr: Address = null

    for (member <- cluster.state.members) {
 //     printf(member.address.port + "\n")
      if (!member.hasRole("frontendapi") && member.address.port.hashCode > keyPos && member.address.port.hashCode < min) {
        min = member.address.port.hashCode
        minAddr = member.address
      }
    }
    // If null -> find the lowest
    if (minAddr == null) {
 //     println("nullem jestem")
      var min = Int.MaxValue

      for (member <- cluster.state.members) {
        if (member.address.port.hashCode <= min && !member.hasRole("frontendapi"))
          {
            min = member.address.port.hashCode()
            minAddr = member.address
          }

      }
    }
    printf("return port: " + minAddr.port + "\n")
    minAddr

*/

    if (nodes.isEmpty) {
      println("No nodes in cluster")
      None
    }

    for (node <- nodes) {
      if (node._1 > key.hashCode())
        return Some(node._2.address)
    }

    Some(nodes.head._2.address)
  }


  // returns true if the current node is the successor of the node with a given address
  def harryYouAreTheChosenOne(addr: Address): Boolean = {
    currentMember.get.address == findSuccessorNode(addr.port).get
  }


  def findPredecessorNode(key: Object): Option[Address] = {
    if (nodes.isEmpty){
      println("No nodes in cluster")
      None
    }
    else {
      var addr = nodes.last._2.address
      for (node <- nodes) {
        if (node._1 > key.hashCode)
          return Some(addr)
        addr = node._2.address
      }
      Some(nodes.last._2.address)
    }
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
            // new node
            if (initNode) {
              log.info("welcome received {}", member.address)
              currentMember = Some(member)
            }
          case _ =>
            // old node
            context.actorSelection(RootActorPath(member.address) / "user" / "clusterListener") ! WelcomeMessage(member)
            nodes = nodes.+((member.address.port.hashCode, member))

            var pack = mutable.Map[String, String]()
            for ((key, value) <- dataMap){
              if (findSuccessorNode(key).get == member.address) {
                pack.put(key, value)
                dataMap.remove(key)
              }
              context.actorSelection(RootActorPath(member.address) / "user" / "clusterListener") ! DataPackage(pack)
              context.actorSelection(RootActorPath(findSuccessorNode(member.address).get) / "user" / "clusterListener") ! DataPackage(pack)
            }


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

      if (harryYouAreTheChosenOne(member.address) && !member.hasRole("frontendapi")) {

        // copy data for which the removed member was the primary node (copy to the successor or current node)
        // if current node is 3 and the removed one was 2
        //    you need to know the hash of node 2 and the data from node 3 (current node)
        //    you copy the data to node 4
        val hashPos = member.address.port.hashCode
        var pack : mutable.Map[String, String] = mutable.Map[String, String]()
        for ((key, value) <- dataMap) {
          if (key.hashCode <= hashPos ||
             (key.hashCode > hashPos) && key.hashCode > nodes.last._2.address.port.hashCode) {
            pack.+((key, value))
          }
        }
        context.actorSelection(RootActorPath(findSuccessorNode(currentMember.get.address.port).get) / "user" / "clusterListener") ! DataPackage(pack)

        // copy data for which the removed member was the backup node (copy to the current node)
        // if current node is 3 and the removed one was 2
        //    you need to know the hash of node 0 and the data from node 1
        //    you copy the data to node 3 (current node)

        // sending request to the predecessor of the removed node (removed node is no longer in 'nodes')
        context.actorSelection(RootActorPath(findPredecessorNode(currentMember.get.address).get) / "user" / "clusterListener") ! DataPackageRequest()
      }


    case DataPackage(pack : mutable.Map[String, String]) =>
      println("Data Package")
      for ((key, value) <- pack) {
        println("key: " + key + " value: " + value)
        dataMap.put(key, value)
      }


    // to find the data for which the removed node was the backup node we have to find all data for which the predecessor of the removed node is the primary node
    case DataPackageRequest() =>
      println("Data Package Request")
      var map : mutable.Map[String, String] = mutable.Map[String, String]()
      for ((key, value) <- dataMap) {
        if (findSuccessorNode(key).get == currentMember.get.address) {
          println("key: " + key + " value: " + value)
          map.put(key, value)
        }
      }
      sender ! DataPackage(map)


    case Add(key: String, value: String) =>
      println("dodawanie " + key + " " + value)

      val minAddr = findSuccessorNode(key).get
      val nextMinAddr = findSuccessorNode(minAddr.port).get

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
      context.actorSelection(RootActorPath(findSuccessorNode(key).get) / "user" / "clusterListener") ! RealGetOne(key, sender)

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


    case Delete(key) =>
      val addr = findSuccessorNode(key).get
      val nextAddr = findSuccessorNode(addr).get
      context.actorSelection(RootActorPath(addr) / "user" / "clusterListener") ! RealDelete(key)
      context.actorSelection(RootActorPath(nextAddr) / "user" / "clusterListener") ! RealDelete(key)


    case RealDelete(key) =>
      dataMap.remove(key)


    //TODO
    case GetQuorum(key) =>
    //TODO


    case _: MemberEvent => // ignore
  }
}