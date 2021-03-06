package akkadb.actor

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Terminated}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.singleton.{ClusterSingletonProxy, ClusterSingletonProxySettings}
import akkadb.message.{Add, BackendRegistration, GetOne, Result}
import com.typesafe.config.ConfigFactory

import scala.util.Random


class FrontendActor extends Actor {

  var databaseBackends: IndexedSeq[ActorRef] = IndexedSeq.empty[ActorRef]
  val cluster = Cluster(context.system)

  val proxy: ActorRef = context.system.actorOf(
    ClusterSingletonProxy.props(
      singletonManagerPath = "/user/frontend-api",
      settings = ClusterSingletonProxySettings(context.system).withRole("frontendapi")),
    name = "consumerProxy")


  override def preStart(): Unit = {
    cluster.subscribe(self, classOf[MemberUp])
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  def receive: PartialFunction[Any, Unit] = {

    case msg: String =>
      println(msg)
      println(databaseBackends.size)

    case addKeyVal: Add =>
      println(databaseBackends.size.toString)

      databaseBackends(Random.nextInt(databaseBackends.size)) forward addKeyVal

    case BackendRegistration if !databaseBackends.contains(sender()) =>
      databaseBackends = databaseBackends :+ sender()
      context watch sender()

    case Terminated(a) =>
      databaseBackends = databaseBackends.filterNot(_ == a)


    case getValue: GetOne =>
      databaseBackends(Random.nextInt(databaseBackends.size)) forward getValue


    case Result(value) =>
      value match {
        case Some(v) => printf("Received value: " + v)
        case _ => printf("No such key in the database")
      }


    case _ =>
    // Ignore
  }
}


object FrontendActor {
  private var _frontend: Option[ActorRef] = None


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
    _frontend = Some(system.actorOf(Props[FrontendActor], name = "frontend-api"))
  }

  def getFrontend: ActorRef = {
    _frontend match {
      case None => initiate()
      case _ => // Ignore
    }

    _frontend.get
  }
}
