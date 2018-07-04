package akkadb.message

import akka.cluster.Member

case class WelcomeMessage(member: Member)
