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

  /*Start of new variables*/
  var kv = Map.empty[String, String]
  var secondaries = Map.empty[ActorRef, ActorRef] //replicators map
  var consistency = Map.empty[String, ActorRef] //ConsistencyManagers. One per key.
  var persister = context.actorOf(persistenceProps)
  var replicators = Set.empty[ActorRef]

  /*End of new variables*/

  // a map from secondary replicas to replicators
  // the current set of replicators

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
      kv +=(key -> value)
      val cmgr = consistency.get(key) getOrElse (context.actorOf(ConsistencyManager.props(replicators, persister), s"con-mgr-$key"))
      cmgr ! Ensure(key, Some(value), id)
    }

    case r @ Remove(key, id) => {
      kv -= key
      val cmgr = consistency.get(key) getOrElse (context.actorOf(ConsistencyManager.props(replicators, persister), s"con-mgr-$key"))
      cmgr ! Ensure(key, None, id)
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

    case Terminated(replica) => secondaries(replica) ! PoisonPill

  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {

    case Get(key, id) => {
      sender ! GetResult(key, kv.get(key), id)
    }

    case s @ Snapshot(key, value, seq) =>
      if (seq > currSeq) ()
      else if (seq < currSeq) sender ! SnapshotAck(key, seq)
      else {
        val cmgr = consistency.get(key) getOrElse (context.actorOf(ConsistencyManager.props(replicators, persister), s"con-mgr-$key"))
        cmgr ! Ensure(key, value, seq)
      }
  }

}

