package akkadb.message

import akka.cluster.Member

import scala.collection.mutable

case class StatusReport(member: Member, dataMap: mutable.HashMap[String, String])