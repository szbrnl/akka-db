package sample.cluster.simple

import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.actor.Props
import akka.cluster.Cluster

object SimpleClusterApp {
  def main(args: Array[String]): Unit = {
    if (args.isEmpty)
      startup(Seq("2551", "2552", "0"))
    else
      startup(args)
  }

  def startup(ports: Seq[String]): Unit = {
    ports foreach { port =>
      // Override the configuration of the port


      // Create an Akka system
 //     val system = ActorSystem("ClusterSystem")

      SimpleClusterListener.initiate(port)
      // Create an actor that handles cluster domain events
//      system.actorOf(Props[SimpleClusterListener], name = "clusterListener")

    }
  }

}

