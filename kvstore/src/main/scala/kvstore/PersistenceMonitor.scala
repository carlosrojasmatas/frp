package kvstore

import akka.actor.Actor
import akka.actor.ActorRef
import kvstore.Replicator.Replicate
import kvstore.Replicator.Replicated
import kvstore.Persistence.Persisted
import akka.actor.Props
import scala.concurrent.duration._
import scala.language.postfixOps
import kvstore.Persistence.Persist
import kvstore.PersistenceMonitor.Fact
import akka.actor.Cancellable

object PersistenceMonitor {

  case class Done(key: String, id: Long)
  case class Fact(key:String,value:Option[String],id:Long)
  case object Retry
  case object ReplicaDown

  def props(replicators: Set[ActorRef], persistor: ActorRef, fact:Fact) = Props(classOf[PersistenceMonitor], replicators, persistor, fact,false)
  def props(replicators: Set[ActorRef],fact:Fact) = Props(classOf[PersistenceMonitor], replicators, null, fact,true)
}

class PersistenceMonitor(replicators: Set[ActorRef], persistor: ActorRef, fact:Fact, replicationOnly:Boolean=false) extends Actor {
  import PersistenceMonitor._

  implicit val ec = context.system.dispatcher

  var retry:Cancellable = _
  
  if(!replicationOnly) {
    retry = context.system.scheduler.schedule(100 milliseconds, 100 milliseconds, self, Retry)
    persistor ! Persist(fact.key, fact.value, fact.id)
  }
  
  replicators foreach (r => r ! Replicate(fact.key, fact.value, fact.id))

  def receive: Receive = this.persist(replicators.size, false)

  def persist(pending: Int, persisted: Boolean): Receive = {

    case Retry => {
      persistor ! Persist(fact.key, fact.value, fact.id)
    }
    
    case ReplicaDown => {
      if(pending -1 == 0 && (persisted || replicationOnly)) context.parent ! Done(fact.key, fact.id)
      else context.become(this.persist(pending -1 , persisted))
    }

    case r @ Replicated(key, id) =>
      {
        if ((pending - 1) == 0) {
          if (persisted || replicationOnly) {
            context.parent ! Done(key, id)
            context.stop(self)
          }
        } else {
          context.become(this.persist(pending - 1, persisted))
        }
      }

    case p @ Persisted(key, id) => {
      retry.cancel()
      if (pending == 0) {
        context.parent ! Done(key, id)
        context.stop(self)
      } else {
        context.become(this.persist(pending , true))
      }
    }
    
  }
}