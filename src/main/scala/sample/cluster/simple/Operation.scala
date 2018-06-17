package sample.cluster.simple

import akka.actor.ActorRef
import io.aeron.driver.Sender

import scala.collection.mutable

case class Add(key: String, value:String)
case class RealAdd(key: String, value: String)
case class GetOne(key: String)
case class GetQuorum(key: String)
case class GetPosition()
case class DataPackage(map: mutable.Map[String, String])
case class DataPackageRequest()
case class RealGetOne(key: String, sender: ActorRef)
case class Result(value: Option[String])