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

  private type RetryOrFail = (Cancellable, Cancellable)
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var persister = context.actorOf(persistenceProps)

  var acks = Map.empty[Long, (ActorRef, Persist)]
  var waitingRoom = Map.empty[Long, RetryOrFail]

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
      replicators foreach (_ ! Replicate(key, Some(value), id))
      kv = kv.updated(key, value)
      acks = acks.updated(id, (sender, persist))
      waitingRoom = waitingRoom.updated(id,
        (context.system.scheduler.schedule(0 milliseconds, 100 milliseconds, self, Retry(id)),
          context.system.scheduler.scheduleOnce(1 second, self, OperationFailed(id))))
    }
    
    case Replicated(key,id)=>
      
      
      
    case Retry(id) =>
      acks.get(id) map {
        case (k, op) => persister ! op
      }
    case of @ OperationFailed(id) =>
      acks(id)._1 ! of
      waitingRoom(id)._1.cancel()
      waitingRoom(id)._2.cancel()
      waitingRoom -= id
      acks -= id

    case Persisted(key, id) =>
      val ref = acks(id)
      ref._1 ! OperationAck(id)
      acks -= id
      waitingRoom(id)._1.cancel()
      waitingRoom(id)._2.cancel()
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
    
    case Terminated(replicator) => replicators = replicators - replicator

  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)

    case Snapshot(key, value, seq) =>
      if (seq > currSeq) ()
      else if (seq < currSeq) sender ! SnapshotAck(key, seq)
      else {
        val p = Persist(key, value, seq)
        persister ! p
        acks = acks.updated(seq, (sender, p))
        waitingRoom = waitingRoom.updated(seq,
          (context.system.scheduler.schedule(0 milliseconds, 100 milliseconds, self, Retry(seq)), //retry every 100 ms and after 1 second fail
            context.system.scheduler.scheduleOnce(1 second, self, OperationFailed(seq))))
        value match {
          case Some(v) => kv = kv.updated(key, v)
          case None    => kv -= key
        }
      }

    case OperationFailed(seq) =>
      acks -= seq
      waitingRoom(seq)._1.cancel()
      waitingRoom(seq)._2.cancel()
      waitingRoom -= seq

    case Persisted(key, id) =>
      val ref = acks(id)
      currSeq += 1
      ref._1 ! SnapshotAck(key, id)
      acks -= id
      waitingRoom(id)._1.cancel()
      waitingRoom(id)._2.cancel()
      waitingRoom -= id

//    case op: Operation          => sender ! OperationFailed(op.id)

    case Retry(id) =>
      acks.get(id) map {
        case (k, op) => persister ! op
      }
  }

}

