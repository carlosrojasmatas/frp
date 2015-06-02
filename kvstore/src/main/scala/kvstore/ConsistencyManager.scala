package kvstore

import akka.actor.Props
import akka.actor.Actor
import kvstore.Persistence.Persisted
import kvstore.Replicator.Replicate
import kvstore.Replicator.Replicated

object ConsistencyManager {
  case class Consistent(key: String, id: Long)
  def props(replicationFactor: Int): Props = Props(classOf[ConsistencyManager], replicationFactor)
}

class ConsistencyManager(replicationFactor: Int) extends Actor {

  import ConsistencyManager._

  def receive: Receive = this.waitForConsistency(replicationFactor, false)

  def waitForConsistency(pending: Int, persisted: Boolean): Receive  = {

    case r @ Replicated(key, id) =>
      {
        if ((pending - 1) == 0) {
          if (persisted) {
            context.parent ! Consistent(key, id)
            context.stop(self)
          }
        } else {
          context.become(this.waitForConsistency(pending - 1, persisted))
        }
      }

    case p @ Persisted(key, id) => {
      if (pending == 0) {
        context.parent ! Consistent(key, id)
        context.stop(self)
      } else {
        context.become(this.waitForConsistency(pending, true))
      }
    }
  }
}
