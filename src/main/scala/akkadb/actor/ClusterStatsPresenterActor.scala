package akkadb.actor

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Terminated}
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.singleton.{ClusterSingletonProxy, ClusterSingletonProxySettings}
import akka.cluster.{Cluster, Member}
import akkadb.message.{BackendRegistration, PrepareReport, SendStatusReport, StatusReport}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable

class ClusterStatsPresenterActor extends Actor {

  var databaseBackends: IndexedSeq[ActorRef] = IndexedSeq.empty[ActorRef]
  val cluster = Cluster(context.system)

  var receivedReports = 0
  var dataSet: Set[String] = Set[String]()
  var nodeDataSet: mutable.HashMap[Member, Set[String]] = mutable.HashMap()

  val proxy: ActorRef = context.system.actorOf(
    ClusterSingletonProxy.props(
      singletonManagerPath = "/user/cluster-stats",
      settings = ClusterSingletonProxySettings(context.system).withRole("clusterstats")),
    name = "consumerProxyStats")

  override def preStart(): Unit = {
    cluster.subscribe(self, classOf[MemberUp])
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  def generateReport(): String = {
    val builder = new mutable.StringBuilder()

    nodeDataSet.foreach(elem =>
      builder.append(elem._1.address.port.get + "  " + elem._2.size + "/" + dataSet.size + " " + elem._1.address.port.get.hashCode() +  "\n" ))

    builder.toString()
  }

  def receive: PartialFunction[Any, Unit] = {

    case BackendRegistration if !databaseBackends.contains(sender()) =>
      println("[INFO] Backend node detected")
      databaseBackends = databaseBackends :+ sender()
      context watch sender()


    case Terminated(a) =>
      databaseBackends = databaseBackends.filterNot(_ == a)


    case StatusReport(member, data) =>
      receivedReports += 1

      dataSet = dataSet ++ data.keys
      nodeDataSet.put(member, data.keys.toSet)

      if (receivedReports == databaseBackends.size)
        println()
      println(generateReport())


    case PrepareReport() =>
      receivedReports = 0
      dataSet = Set[String]()
      nodeDataSet = mutable.HashMap[Member, Set[String]]()

      databaseBackends.foreach(x => x ! SendStatusReport())

    case _ => // Ignore
  }
}


object ClusterStatsPresenterActor {
  private var _stats: Option[ActorRef] = None

  def initiate(): Unit = {
    val port = "0"
    val config = ConfigFactory.parseString(
      s"""
        akka.remote.netty.tcp.port=$port
        akka.remote.artery.canonical.port=$port
        """)
      .withFallback(ConfigFactory.parseString("akka.cluster.roles = [clusterstats]"))
      .withFallback(ConfigFactory.load())

    val system = ActorSystem("ClusterSystem", config)

    // Create an actor that handles cluster domain events
    _stats = Some(system.actorOf(Props[ClusterStatsPresenterActor], name = "cluster-stats"))
  }

  def prepareReport(): Unit = {
    _stats match {
      case None => initiate()
      case _ => // Ignore
    }

    _stats.get ! PrepareReport()
  }
}
