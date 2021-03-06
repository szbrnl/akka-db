package akkadb.actor

import akka.actor.{Actor, ActorLogging, ActorRef, Address, RootActorPath}
import akka.cluster.ClusterEvent._
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.{Cluster, Member, MemberStatus}
import akkadb.message._

import scala.collection.mutable


class ClusterListener extends Actor with ActorLogging {
  val cluster = Cluster(context.system)
  val mediator: ActorRef = DistributedPubSub(context.system).mediator

  var nodes: mutable.SortedMap[Int, Member] = mutable.SortedMap[Int, Member]()
  var currentMember: Option[Member] = None

  var initNode: Boolean = false

  var dataMap: mutable.HashMap[String, String] = mutable.HashMap[String, String]()


  // subscribe to cluster changes, re-subscribe when restart
  override def preStart(): Unit = {
    // Determine if it's first node in the cluster
    if (cluster.state.members.count(_.status == MemberStatus.up) == 1) {
      initNode = true
    }

    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive: PartialFunction[Any, Unit] = {
    case msg: String =>
      log.info(msg)
      println(currentMember.get.address)


    case MemberUp(member) =>
      log.info("Member is Up: {}", member.address)

      if (member.hasRole("frontendapi")) {
        context.actorSelection(RootActorPath(member.address) / "user" / "frontend-api") !
          BackendRegistration
      }
      else if (member.hasRole("clusterstats")) {
        context.actorSelection(RootActorPath(member.address) / "user" / "cluster-stats") !
          BackendRegistration
      }
      else {
        currentMember match {
          case None =>

            // new node
            if (initNode) {
              log.info("welcome received {}", member.address)
              currentMember = Some(member)
              nodes = nodes.+((member.address.port.get.hashCode(), member))
            }
            else {
              nodes = nodes.+((member.address.port.get.hashCode(), member))
            }


          case _ =>
            // old node
            context.actorSelection(RootActorPath(member.address) / "user" / "clusterListener") ! WelcomeMessage(member)
            nodes = nodes.+((member.address.port.get.hashCode, member))

            // New node detected
            val pack = mutable.Map[String, String]()
            for ((key, value) <- dataMap) {
              if (findSuccessorNode(key.hashCode).get == member.address) {
                pack.put(key, value)
                dataMap.remove(key)
              }
            }
            println("[INFO] Migrating data")
            if (member.address != currentMember.get.address)
              context.actorSelection(RootActorPath(member.address) / "user" / "clusterListener") ! DataPackage(pack)
            if (member.address != findSuccessorNode(member.address.port.get.hashCode()).get && findSuccessorNode(member.address.port.get.hashCode()).get != currentMember.get.address)
              context.actorSelection(RootActorPath(findSuccessorNode(member.address.port.get.hashCode()).get) / "user" / "clusterListener") ! DataPackage(pack)
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
      nodes = nodes.-(member.address.port.get.hashCode)


      if (harryYouAreTheChosenOne(member.address) && !member.hasRole("frontendapi")) {

        // copy data for which the removed member was the primary node (copy to the successor or current node)
        // if current node is 3 and the removed one was 2
        //    you need to know the hash of node 2 and the data from node 3 (current node)
        //    you copy the data to node 4
        val hashPos = member.address.port.get.hashCode
        val pack: mutable.Map[String, String] = mutable.Map[String, String]()
        for ((key, value) <- dataMap) {
          print(key, value, key.hashCode, hashPos)
          if (key.hashCode <= hashPos ||
            (key.hashCode > hashPos) && key.hashCode > nodes.last._2.address.port.get.hashCode) {
            pack.put(key, value)
          }
        }
        println(pack)
        context.actorSelection(RootActorPath(findSuccessorNode(currentMember.get.address.port.get.hashCode()).get) / "user" / "clusterListener") ! DataPackage(pack)

        // copy data for which the removed member was the backup node (copy to the current node)
        // if current node is 3 and the removed one was 2
        //    you need to know the hash of node 0 and the data from node 1
        //    you copy the data to node 3 (current node)

        // sending request to the predecessor of the removed node (removed node is no longer in 'nodes')
        context.actorSelection(RootActorPath(findPredecessorNode(currentMember.get.address.port.get).get) / "user" / "clusterListener") ! DataPackageRequest()
      }


    case DataPackage(pack: mutable.Map[String, String]) =>
      println("Data Package")
      for ((key, value) <- pack) {
        println("key: " + key + " value: " + value)
        dataMap.put(key, value)
      }


    // to find the data for which the removed node was the backup node we have to find all data for which the predecessor of the removed node is the primary node
    case DataPackageRequest() =>
      println("Data Package Request")
      var map: mutable.Map[String, String] = mutable.Map[String, String]()
      for ((key, value) <- dataMap) {
        if (findSuccessorNode(key.hashCode).get == currentMember.get.address) {
          println("key: " + key + " value: " + value)
          map.put(key, value)
        }
      }
      sender ! DataPackage(map)


    case Add(key: String, value: String) =>

      println("Adding " + key + " " + value + "  " + key.hashCode)

      val minAddr = findSuccessorNode(key.hashCode).get
      val nextMinAddr = findSuccessorNode(minAddr.port.get.hashCode() + 1).get

      context.actorSelection(RootActorPath(minAddr) / "user" / "clusterListener") ! RealAdd(key, value)

      if (minAddr != nextMinAddr)
        context.actorSelection(RootActorPath(nextMinAddr) / "user" / "clusterListener") ! RealAdd(key, value)


    case RealAdd(key: String, value: String) =>
      println("[NODE " + currentMember.get.address.port.get.hashCode() + "]")
      println("Added: " + key + " " + value)
      dataMap.put(key, value)
      for ((key, value) <- dataMap)
        printf(key + " " + value + "\n")


    case GetOne(key) =>
      context.actorSelection(RootActorPath(findSuccessorNode(key.hashCode).get) / "user" / "clusterListener") ! RealGetOne(key, sender)


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


    case SendStatusReport() =>
      println("Sending status report")
      sender ! StatusReport(currentMember.get, dataMap)


    case _: MemberEvent => // ignore
  }

  def findSuccessorNode(keyHashCode: Int): Option[Address] = {
    nodes.dropWhile(_._1 <= keyHashCode) match {
      case x if x.isEmpty =>
        Some(nodes.head._2.address)
      case x =>
        Some(x.head._2.address)
    }
  }


  // returns true if the current node is the successor of the node with a given address
  def harryYouAreTheChosenOne(addr: Address): Boolean = {
    currentMember.get.address == findSuccessorNode(addr.port.get.hashCode()).get
  }


  def findPredecessorNode(key: Int): Option[Address] = {
    nodes.takeWhile(_._1 < key.hashCode()) match {
      case x if x.isEmpty =>
        Some(nodes.last._2.address)
      case x =>
        Some(x.last._2.address)
    }
  }

}