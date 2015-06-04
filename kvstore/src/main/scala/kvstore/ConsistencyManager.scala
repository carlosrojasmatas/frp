package kvstore

import akka.actor.Props
import akka.actor.Actor
import kvstore.Persistence.Persist
import kvstore.Replicator.Replicate
import kvstore.Replicator.Replicated
import akka.actor.ActorRef
import scala.collection.immutable.Queue
import kvstore.Persistence.Persisted
import kvstore.Replicator.SnapshotAck
import akka.actor.Cancellable
import scala.concurrent.duration._
import scala.language.postfixOps
import kvstore.Replica.OperationFailed
import kvstore.Replicator.SnapshotAck

object ConsistencyManager {
  case class Consistent(key: String, id: Long)
  case class Ensure(key: String, value: Option[String], id: Long, sender: ActorRef, response: () => Any)
  case class ReplicaAdded(rep: ActorRef)
  case class ReplicaGone(rep: ActorRef)
  case object Retry
  case object Timeout
  def props(replicationFactor: Int): Props = Props(classOf[ConsistencyManager], replicationFactor)
  def props(replicators: Set[ActorRef], persister: ActorRef): Props = Props(classOf[ConsistencyManager], replicators, persister)
}

class ConsistencyManager(var replicators: Set[ActorRef], persister: ActorRef) extends Actor {

  import ConsistencyManager._

  private var workingList: Queue[Ensure] = Queue.empty
  private var retries: Set[Cancellable] = Set.empty[Cancellable]
  private var timeouts: Map[String, Cancellable] = Map.empty[String, Cancellable]
  implicit val ec = context.system.dispatcher

  def receive: Receive = {
    case e: Ensure =>
      workingList = workingList.enqueue(e)
      context.become(next)
    case a: ReplicaAdded => replicators += a.rep
    case a: ReplicaGone  => replicators -= a.rep
  }

  def next: Receive = {
    if (workingList.size > 0) {
      val newState = workingList.dequeue
      workingList = newState._2
      processing(newState._1)
    } else receive
  }

  def processing(e: Ensure): Receive = {
    persister ! Persist(e.key, e.value, e.id)
    retries += context.system.scheduler.scheduleOnce(100 milliseconds, self, Retry)
    timeouts += (e.key -> context.system.scheduler.scheduleOnce(1 second, self, Timeout))
    replicators foreach (_ ! Replicate(e.key, e.value, e.id))
    waitForConsistency(replicators, false, e)
  }

  def waitForConsistency(pendingReplicators: Set[ActorRef], persisted: Boolean, e: Ensure): Receive = {

    case r @ Replicated(key, id) =>
      {
        if ((pendingReplicators - sender).size == 0) {
          if (persisted) {
            timeouts(key).cancel
            e.sender ! e.response()
            context.become(next)
          }
        } else {
          context.become(this.waitForConsistency(pendingReplicators - sender, persisted, e))
        }
      }

    case p @ Persisted(key, id) => {
      if (pendingReplicators.size == 0) {
        timeouts(key).cancel
        e.sender ! e.response()
        context.become(next)
      } else {
        context.become(this.waitForConsistency(pendingReplicators, true, e))
      }
    }

    case Retry =>
      retries += context.system.scheduler.scheduleOnce(100 milliseconds, self, Retry)
      persister ! Persist(e.key, e.value, e.id)

    case Timeout =>
      e.sender ! OperationFailed(e.id)
      context.become(next)

    case e: Ensure =>
      workingList = workingList.enqueue(e)

    case a: ReplicaAdded =>
      a.rep ! Replicate(e.key, e.value, e.id)
      context.become(waitForConsistency(pendingReplicators + a.rep, persisted, e))
      replicators += a.rep

    case b: ReplicaGone =>
      replicators -= b.rep
      val nr = pendingReplicators - b.rep
      if (nr.size == 0)
        if (persisted) {
          e.sender ! e.response()
          context.become(next)
        } else {
          context.become(waitForConsistency(nr, persisted, e))
        }
      else {
        context.become(waitForConsistency(nr, persisted, e))
      }

  }
}
