package week5

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.Props
import akka.actor.ActorRef
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.actor.Terminated

object Controller {
  case class Check(url:String,depth:Int)
  case class Result(rs:Set[String])
  case object Timeout
}

class Controller extends Actor with ActorLogging{
  import Controller._
  
  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries=5){
    case _:Exception => SupervisorStrategy.Restart  
  }
  
  var cache = Set.empty[String]
  
  def receive = { 
    case Check(url,depth)=> {
      log.debug("{} checking {}",depth,url)
      if(!cache(url) && depth > 0)
        context.watch(context.actorOf(Props(new Getter(url,depth-1))))
      cache += url
    }
    case Getter.Done => 
    case Terminated(_)=> if(context.children.isEmpty) context.parent ! Result(cache)
    case Timeout => context.children foreach (_ ! Getter.Abort)
  }

}