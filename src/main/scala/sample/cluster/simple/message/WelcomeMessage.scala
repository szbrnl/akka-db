package sample.cluster.simple.message

import akka.cluster.Member

case class WelcomeMessage(member: Member)
