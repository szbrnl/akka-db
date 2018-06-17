package sample.cluster.simple

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Terminated}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings, ClusterSingletonProxy, ClusterSingletonProxySettings}
import com.typesafe.config.ConfigFactory
import sample.cluster.transformation.BackendRegistration

import scala.util.Random


class ClusterStatsPresenterActor extends Actor {

  var databaseBackends: IndexedSeq[ActorRef] = IndexedSeq.empty[ActorRef]
  val cluster = Cluster(context.system)

  val proxy: ActorRef = context.system.actorOf (
    ClusterSingletonProxy.props (
      singletonManagerPath = "/user/cluster-stats",
      settings = ClusterSingletonProxySettings (context.system).withRole ("clusterstats") ),
    name = "consumerProxy")



  override def preStart(): Unit = {
    cluster.subscribe(self, classOf[MemberUp])
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  def receive = {

    case BackendRegistration if !databaseBackends.contains(sender()) =>
      println("hellooooooo")
      databaseBackends = databaseBackends :+ sender()
      context watch sender()

    case Terminated(a) =>
      databaseBackends = databaseBackends.filterNot(_ == a)

    case Result(value) =>
      value match {
        case Some(value) => printf("Received value: " + value)
        case _ => printf("No such key in the database")
      }

    case _ =>
      printf("No match")
  }
}


object ClusterStatsPresenterActor {
  private var _stats: ActorRef = _
  private var _proxy:ActorRef = _


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
    _stats = system.actorOf(Props[FrontendActor], name = "cluster-stats")
  }

  def getFrontend() = _stats
  def getProxy() = _proxy
}
