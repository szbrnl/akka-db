package sample.cluster.simple

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Terminated}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings, ClusterSingletonProxy, ClusterSingletonProxySettings}
import com.typesafe.config.ConfigFactory
import sample.cluster.transformation.BackendRegistration

import scala.util.Random


class FrontendActor extends Actor {

  var databaseBackends = IndexedSeq.empty[ActorRef]
  val cluster = Cluster(context.system)

  val proxy = context.system.actorOf (
    ClusterSingletonProxy.props (
      singletonManagerPath = "/user/frontend-api",
      settings = ClusterSingletonProxySettings (context.system).withRole ("frontendapi") ),
    name = "consumerProxy")



  override def preStart(): Unit = {
    cluster.subscribe(self, classOf[MemberUp])
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  def receive = {

    case msg: String =>
      println(msg)
      databaseBackends(Random.nextInt(databaseBackends.size)) forward msg

    case addKeyVal: Add =>
      println(databaseBackends.size.toString)
      databaseBackends(Random.nextInt(databaseBackends.size)) forward addKeyVal

    case BackendRegistration if !databaseBackends.contains(sender()) =>
      println("hellooooooo")
      databaseBackends = databaseBackends :+ sender()
      context watch sender()

    case Terminated(a) =>
      databaseBackends = databaseBackends.filterNot(_ == a)
  }
}


object FrontendActor {
  private var _frontend: ActorRef = _
  private var _proxy:ActorRef = _


  //  def initiate() = {
  //    val config = ConfigFactory.load().getConfig("FrontendAPI")
  //
  //    val system = ActorSystem("ClusterSystem", config)
  //
  //    _frontend = system.actorOf(Props[FrontendActor], name = "frontend-api")
  //
  //  }

  def initiate(): Unit = {
    val port = "0"
    val config = ConfigFactory.parseString(
      s"""
        akka.remote.netty.tcp.port=$port
        akka.remote.artery.canonical.port=$port
        """)
      .withFallback(ConfigFactory.parseString("akka.cluster.roles = [frontendapi]"))
      .withFallback(ConfigFactory.load())

    val system = ActorSystem("ClusterSystem", config)
    // Create an actor that handles cluster domain events
    _frontend = system.actorOf(Props[FrontendActor], name = "frontend-api")

//    val system = ActorSystem("ClusterSystem", config)
//    _frontend = system.actorOf(
//      ClusterSingletonManager.props(
//        singletonProps = Props(classOf[FrontendActor]),
//        terminationMessage = End,
//        settings = ClusterSingletonManagerSettings(system).withRole("frontendapi")),
//      name = "frontend-api"
//    )
//
//    _proxy = system.actorOf (
//      ClusterSingletonProxy.props (
//        singletonManagerPath = "/user/consumer",
//        settings = ClusterSingletonProxySettings (system).withRole ("frontend-api") ),
//      name = "consumerProxy")
//    //      Props[FrontendActor], name = "frontend")
  }

  def getFrontend() = _frontend
  def getProxy() = _proxy
}
