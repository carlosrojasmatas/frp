package kvstore

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Right
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.AllForOneStrategy
import akka.actor.Cancellable
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.SupervisorStrategy
import akka.actor.SupervisorStrategy._
import akka.actor.Terminated
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout
import kvstore.Arbiter._
import com.sun.org.apache.xerces.internal.impl.dtd.models.CMAny

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
      kv += (key -> value)
      val cmgr = managerFor(key)
      cmgr ! Ensure(key, Some(value), id, sender, () => OperationAck(id))
    }

    case r @ Remove(key, id) => {
      kv -= key
      val cmgr = consistency.get(key) getOrElse (context.actorOf(ConsistencyManager.props(replicators, persister), s"con-mgr-$key"))
      cmgr ! Ensure(key, None, id, sender, () => OperationAck(id))
    }

    case g @ Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case msg@Replicas(replicas) =>
      val replicaSet = replicas - self
      val removed = secondaries.keySet &~ replicaSet
      val added = replicaSet &~ secondaries.keySet
      removed.foreach { r =>
        {
          //go over all pendings acks and confirm them once

          consistency.foreach {
            case (key, mgr) => mgr ! ReplicaGone(secondaries(r))
          }
          secondaries(r) ! PoisonPill //stop the replicator
          secondaries -= r //removed from reference
        }
      }

      added.foreach { a: ActorRef =>
        val replicator = context.actorOf(Replicator.props(a))
        replicators += replicator
        secondaries = secondaries.updated(a, replicator)
        context.watch(a)
        kv.zipWithIndex.foreach {
          case (k, v) => {
            val cMgr = managerFor(k._1)
            cMgr ! ReplicaAdded(replicator)
            cMgr ! Ensure(k._1, Some(k._2), v,self,()=>OperationAck(v))
          }
        }
      }

    case Terminated(replica) => secondaries(replica) ! PoisonPill

  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {

    case g @ Get(key, id) => {
      sender ! GetResult(key, kv.get(key), id)
    }

    case s @ Snapshot(key, value, seq) =>
      if (seq > currSeq) ()
      else if (seq < currSeq) sender ! SnapshotAck(key, seq)
      else {
        value match {
          case Some(v) => kv += (key -> v)
          case None => kv -= key
        }
        val cmgr = managerFor(key)
        cmgr ! Ensure(key, value, seq, sender, () => SnapshotAck(key, seq))
        currSeq +=1
      }
  }

  private def managerFor(key: String): ActorRef = {
    consistency.get(key) getOrElse {
      val nc = context.actorOf(ConsistencyManager.props(replicators, persister), s"con-mgr-$key")
      consistency += (key -> nc)
      nc
    }

  }

}

