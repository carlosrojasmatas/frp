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

object PersistenceMonitor {

  case class Done(key: String, id: Long)
  case class Fact(key:String,value:Option[String],id:Long)
  case object Retry

  def props(replicators: Set[ActorRef], persistor: ActorRef, fact:Fact) = Props(classOf[PersistenceMonitor], replicators, persistor, fact)
}

class PersistenceMonitor(replicators: Set[ActorRef], persistor: ActorRef, fact:Fact) extends Actor {
  import PersistenceMonitor._

  implicit val ec = context.system.dispatcher

  val retry = context.system.scheduler.schedule(100 milliseconds, 100 milliseconds, self, Retry)
  println(s"starting monitor with size ${replicators.size}")
  persistor ! Persist(fact.key, fact.value, fact.id)
  replicators foreach (r => r ! Replicate(fact.key, fact.value, fact.id))

  def receive: Receive = this.persist(replicators.size, false)

  def persist(pending: Int, persisted: Boolean): Receive = {

    case Retry => {
      persistor ! Persist(fact.key, fact.value, fact.id)
    }

    case r @ Replicated(key, id) =>
      {
        println(s"replicated $id with pending $pending and replicated = $persisted ")
        if ((pending - 1) == 0) {
          if (persisted) {
            println("sending done")
            context.parent ! Done(key, id)
            context.stop(self)
          }
        } else {
          context.become(this.persist(pending - 1, persisted))
        }
      }

    case p @ Persisted(key, id) => {
      println(s"persisted $id")
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