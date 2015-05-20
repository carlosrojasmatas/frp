package week5

import akka.actor.Actor
import akka.actor.Props
import scala.concurrent.duration._
import scala.language.postfixOps
import akka.actor.ReceiveTimeout

class Main extends Actor {

  import Recepcionist._

  val recepcionist = context.actorOf(Props[Recepcionist], "recepcioninst")

  recepcionist ! Get("http://www.google.com")

  context.setReceiveTimeout(10 seconds)

  def receive = {
    case Result(url, set) =>
      println(set.toVector.sorted.mkString(s"Results for $url:\n", "\n", "\n"))
    case Failed(url) =>
      println(s"Failed to fetch '$url'\n")
    case ReceiveTimeout => context.stop(self)

  }

  override def postStop(): Unit = {
    WebClient.shutdown()
  }

}