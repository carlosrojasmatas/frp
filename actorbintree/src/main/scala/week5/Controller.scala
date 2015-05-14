package week5

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.Props
import akka.actor.ActorRef

object Controller {
  case class Check(url:String,depth:Int)
  case class Result(rs:Set[String])
  case object Timeout
}

class Controller extends Actor with ActorLogging{
  import Controller._
  
  var cache = Set.empty[String]
  var children = Set.empty[ActorRef]
  
  def receive = { 
    case Check(url,depth)=> {
      log.debug("{} checking {}",depth,url)
      if(!cache(url) && depth > 0)
        children += context.actorOf(Props(new Getter(url,depth-1)))
      cache += url
    }
    case Getter.Done => 
      children -= sender
      if(children.isEmpty) context.parent ! Result(cache)
    case Timeout => children foreach (_ ! Getter.Abort)
  }

}