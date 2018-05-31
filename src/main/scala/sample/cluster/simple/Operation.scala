package sample.cluster.simple

case class Add(key: String, value:String)
case class GetOne(key: String)
case class GetQuorum(key: String)

