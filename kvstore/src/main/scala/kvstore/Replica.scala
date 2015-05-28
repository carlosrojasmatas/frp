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
import kvstore.PersistenceMonitor.Done
import kvstore.PersistenceMonitor.Fact
import kvstore.PersistenceMonitor.Done

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

  var monitors = Map.empty[Long, ActorRef]

  var acks = Map.empty[Long, ActorRef]
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

  val leader: Receive = {

    case i @ Insert(key, value, id) => {

      val monitor = context.actorOf(PersistenceMonitor.props(replicators, persister, Fact(key, Some(value), id)))
      waitingRoom = waitingRoom.updated(id, context.system.scheduler.scheduleOnce(1 second, self, OperationFailed(id)))
      acks = acks.updated(id, sender)
      kv = kv.updated(key, value)

    }

    case r @ Replicated(key, id) =>
      println("wrooongggg")
      monitors(id) ! r

    case Done(key, id) =>
      waitingRoom(id).cancel()
      acks(id) ! OperationAck(id)
      waitingRoom -= id
      acks -= id

    case of @ OperationFailed(id) =>
      acks(id) ! of
      waitingRoom -= id
      acks -= id

    case Remove(key, id) => {
      val monitor = context.actorOf(PersistenceMonitor.props(replicators, persister, Fact(key, None, id)))
      waitingRoom = waitingRoom.updated(id, context.system.scheduler.scheduleOnce(1 second, self, OperationFailed(id)))
      acks = acks.updated(id, sender)
      kv = kv - key
    }

    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case Replicas(replicas) => replicas foreach { r =>
      if (!r.equals(self)) {
        val replicator = context.actorOf(Replicator.props(r))
        context.watch(replicator)
//        kv.zipWithIndex.foreach(kv => {
//          replicator ! Replicate(kv._1._1, Some(kv._1._2), kv._2)
//        })
        replicators += replicator
      }
    }

    case Terminated(replicator) => replicators = replicators - replicator

  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)

    case Snapshot(key, value, seq) =>
      println(s"current status $currSeq - $seq")
      if (seq > currSeq) ()
      else if (seq < currSeq) sender ! SnapshotAck(key, seq)
      else {
        println("ok, got it ")
        val monitor = context.actorOf(PersistenceMonitor.props(replicators, persister, Fact(key, value, seq)))
        waitingRoom = waitingRoom.updated(seq, context.system.scheduler.scheduleOnce(1 second, self, OperationFailed(seq)))
        acks = acks.updated(seq, sender)
        value match {
          case Some(v) => kv = kv.updated(key, v)
          case None    => kv -= key
        }

      }

    case OperationFailed(seq) =>
      acks -= seq
      waitingRoom(seq).cancel()
      waitingRoom -= seq

    case Done(key, id) =>
      currSeq += 1
      acks(id) ! SnapshotAck(key, id)
      acks -= id
      waitingRoom(id).cancel()
      waitingRoom -= id

  }

}

