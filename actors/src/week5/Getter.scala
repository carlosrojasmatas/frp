package week5

import akka.actor.Actor
import akka.pattern.pipe
import akka.actor.Status
import scala.concurrent.duration._
import scala.language.postfixOps


object Getter {
  case object Done
  case object Abort
}

class Getter(url:String,depth:Int) extends Actor{

  import Getter._
  
  implicit val exec = context.dispatcher
  
  context.setReceiveTimeout(10 seconds)
  
  WebClient get url pipeTo self
  
  def receive = {
    case body:String => {
      for(link <- WebClient.findLinks(body))
         context.parent ! Controller.Check(link,depth) 
      stop()
    }
    case _: Status.Failure => stop()
  }
  
  def stop():Unit = {
    context.parent ! Done
  }

}package week5

class Getter {

}