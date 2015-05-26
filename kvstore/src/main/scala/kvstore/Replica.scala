package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor }
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.actor.SupervisorStrategy._
import akka.util.Timeout
import akka.actor.AllForOneStrategy
import scala.language.postfixOps
import akka.actor.Cancellable

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply
  case class Retry(id: Long)

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var persister = context.actorOf(persistenceProps)

  var acks = Map.empty[Long, (ActorRef, Persist)]
  var waitingRoom = Map.empty[Long, Cancellable]

  override val supervisorStrategy = AllForOneStrategy(10, 1 second) {
    case e: PersistenceException => {
      Resume
    }
  }

  var currSeq = 0L

  override def preStart() = {
    arbiter ! Join
  }

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case i @ Insert(key, value, id) => {
      val persist = Persist(key, Some(value), id)
      persister ! persist
      kv = kv.updated(key, value)
      acks = acks.updated(id, (sender, persist))
      waitingRoom = waitingRoom.updated(id, context.system.scheduler.schedule(0 milliseconds, 100 milliseconds, self, Retry(id)))
    }

    case Retry(id) =>
      acks.get(id) map {
        case (k, op) => persister ! op
      }

    case Persisted(key, id) =>
      val ref = acks(id)
      ref._1 ! OperationAck(id)
      acks -= id
      waitingRoom(id).cancel()
      waitingRoom -= id

    case Remove(key, id) => {
      kv = kv - key
      sender ! OperationAck(id)
    }

    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case Replicas(replicas) => replicas foreach { r =>
      val replicator = context.actorOf(Replicator.props(r))
      context.watch(replicator)
      kv.zipWithIndex.foreach(kv => replicator ! Replicate(kv._1._1, Some(kv._1._2), kv._2))
      replicators += replicator
    }

  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)

    case Retry =>
      acks foreach {
        case (k, v) => v._1 ! v._2
      }

    case Snapshot(key, value, seq) =>
      if (seq > currSeq) ()
      else if (seq < currSeq) sender ! SnapshotAck(key, seq)
      else {
        val p = Persist(key, value, seq)
        currSeq = currSeq + 1
//        persister ! p
        acks = acks.updated(seq, (sender, p))
        sender ! SnapshotAck(key, seq)
//        waitingRoom = waitingRoom.updated(seq, context.system.scheduler.schedule(0 milliseconds, 100 milliseconds, self, Retry(seq)))
        value match {
          case Some(v) => kv = kv.updated(key, v)
          case None    => kv -= key
        }
      }

    case Persisted(key, id) =>
      val ref = acks(id)
      currSeq = id
      ref._1 ! SnapshotAck(key, id)
      acks -= id
      waitingRoom(id).cancel()
      waitingRoom -= id

    case op: Operation          => sender ! OperationFailed(op.id)
    case Terminated(replicator) => replicators = replicators - replicator

    case Retry(id) =>
      acks.get(id) map {
        case (k, op) => persister ! op
      }
  }

}

