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
import kvstore.PersistenceMonitor.Fact
import kvstore.PersistenceMonitor.ReplicaDown
import scala.util.Right

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
  import scala.language.reflectiveCalls
  import scala.language.implicitConversions
  import ConsistencyManager._
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var persister = context.actorOf(persistenceProps)

  case class SnapshotRequest(ref: ActorRef, request: Snapshot)
  case class UpdateRequest(ref: ActorRef, request: Either[Insert, Remove])
  case class Timeout(key: String, seq: Long)

  implicit def eitherOpToKey(either: Either[Insert, Remove]): String = {
    either match {
      case Right(r) => r.key
      case Left(i)  => i.key
    }
  }

  var pendingAcks = Map.empty[String, SnapshotRequest]
  var acks = Map.empty[Long, UpdateRequest]
  var consistencyMap = Map.empty[Long, ActorRef]
  var timeoutTriggers = Map.empty[Long, Cancellable]
  var retrys = Map.empty[Long, Cancellable]

  override val supervisorStrategy = AllForOneStrategy(10, 1 second) {
    case e: PersistenceException => {
      e.printStackTrace()
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
      timeoutTriggers = timeoutTriggers updated (id, context.system.scheduler.scheduleOnce(1 second, self, Timeout(key, id)))
      consistencyMap = consistencyMap updated (id, context.actorOf(ConsistencyManager.props(replicators.size)))
      persister ! Persist(key, Some(value), id)
      retrys = retrys updated (id, context.system.scheduler.schedule(100 milliseconds, 100 milliseconds, self, Retry(id)))
      acks = acks.updated(id, UpdateRequest(sender, Left(i)))
      replicators foreach { r =>
        r ! Replicate(key, Some(value), id)
      }
    }

    case Retry(id) =>
      acks get (id) map {

        req =>
          req.request match {
            case Left(i)  => persister ! Persist(i.key, Some(i.value), i.id)
            case Right(r) => persister ! Persist(r.key, None, r.id)
          }
      }

    case p @ Persisted(key, id) =>
      retrys(id).cancel
      retrys -= id
      consistencyMap(id) ! p

    case r @ Replicated(key, id) =>
      consistencyMap(id) ! r

    case Consistent(key, id) =>
      timeoutTriggers(id).cancel
      timeoutTriggers -= id
      acks(id).ref ! OperationAck(id)

      acks(id).request match {
        case Left(i) =>
          kv = kv.updated(key, i.value)
        case Right(r) =>
          kv -= key
      }

      acks -= id

    case timeout @ Timeout(key, id) =>
      acks(id).ref ! OperationFailed(id)
      timeoutTriggers -= id
      consistencyMap -= id
      acks -= id

    case r @ Remove(key, id) => {
      timeoutTriggers = timeoutTriggers updated (id, context.system.scheduler.scheduleOnce(1 second, self, Timeout(key, id)))
      consistencyMap = consistencyMap updated (id, context.actorOf(ConsistencyManager.props(replicators.size)))
      acks = acks.updated(id, UpdateRequest(sender, Right(r)))
      val p = Persist(key, None, id)
      retrys = retrys updated (id, context.system.scheduler.schedule(100 milliseconds, 100 milliseconds, persister, p))
      persister ! p
      replicators foreach { r =>
        r ! Replicate(key, None, id)
      }
    }

    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case Replicas(replicas) =>
      val removed = secondaries.keySet &~ replicas.tail
      val added = replicas.tail &~ secondaries.keySet
      
      removed.foreach { r =>
        {
          //go over all pendings acks and confirm them once
          acks foreach {
            case (id, v) => {
              val cmgr = consistencyMap(id)
              val key: String = v.request
              cmgr ! Replicated(key, id)
            }
          }
          //          r ! PoisonPill //Stop the replica
          secondaries(r) ! PoisonPill
          secondaries -= r //removed from reference
        }
      }
      added.foreach { a: ActorRef =>
        val replicator = context.actorOf(Replicator.props(a))
        secondaries = secondaries.updated(a, replicator)
        context.watch(replicator)
        replicators += replicator
        kv.zipWithIndex.foreach {
          case (k, v) => {
            replicator ! Replicate(k._1, Some(k._2), v)
          }
        }
      }

    case Terminated(replicator) => replicators = replicators - replicator

  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {

    case Get(key, id) => {
      pendingAcks get (key) match {
        case Some(v) => sender ! GetResult(key, v.request.valueOption, id)
        case None    => sender ! GetResult(key, kv.get(key), id)
      }

    }

    case s @ Snapshot(key, value, seq) =>
      if (seq > currSeq) ()
      else if (seq < currSeq) sender ! SnapshotAck(key, seq)
      else {
        val p = Persist(key, value, seq)
        persister ! p
        retrys = retrys updated (seq, context.system.scheduler.schedule(100 milliseconds, 100 milliseconds, persister,p))
        pendingAcks = pendingAcks.updated(key, SnapshotRequest(sender, s))
        timeoutTriggers = timeoutTriggers.updated(seq, context.system.scheduler.scheduleOnce(1 second, self, Timeout(key, seq)))
      }

    case Timeout(key, seq) =>
      timeoutTriggers -= seq
      pendingAcks -= key

    case p: Persisted =>
      currSeq = p.id + 1
      retrys(p.id).cancel
      retrys -= p.id
      timeoutTriggers(p.id).cancel()
      timeoutTriggers -= p.id
      val origin = pendingAcks(p.key)
      origin.ref ! SnapshotAck(origin.request.key, origin.request.seq)
      pendingAcks -= p.key
      origin.request.valueOption match {
        case Some(v) => kv = kv.updated(origin.request.key, v)
        case None    => kv -= origin.request.key
      }

  }

}

