package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import scala.concurrent.duration._
import akka.actor.Terminated
import scala.language.postfixOps
import akka.actor.Cancellable

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)
  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher

  private case object Flush
  private case class ReplicationFailed(seq: Long)
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  var timeouts = Map.empty[Long, Cancellable]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  val resender = context.system.scheduler.schedule(100 milliseconds, 100 milliseconds, self, Flush)
  //  context.system.scheduler.schedule(0 milliseconds, 100 milliseconds, self, Retry)

  
  override def postStop(){
    resender.cancel()
  }
  /* TODO Behavior for the Replicator. */
  def receive: Receive = {

    case r @ Replicate(key, valueOption, id) => {
      val seq = nextSeq
      acks = acks.updated(seq, (sender, r))
      pending = batch(Replicator.Snapshot(key, valueOption, seq))
      timeouts += (seq -> context.system.scheduler.scheduleOnce(1 second, self, ReplicationFailed(seq)))
    }

    case ReplicationFailed(seq) =>
      acks -= seq
      timeouts -= seq

    case SnapshotAck(key, seq) => {
      timeouts(seq).cancel
      timeouts = timeouts - seq
      val sender =  acks(seq)._1
      val id =  acks(seq)._2.id
      sender ! Replicated(key, id)
      acks = acks - seq
    }

    case Flush =>
      pending ++ retransmitables foreach (replica ! _)
      pending = Vector.empty[Snapshot]
    case Retry =>
      retransmitables foreach (replica ! _)
    case Terminated(t) => context.stop(self)

  }

  private def retransmitables: Vector[Snapshot] = {
    val s = for {
      seq <- timeouts.keySet
      rep <- acks.get(seq)
      if (pending.indexWhere { seq == _.seq } == -1)
    } yield {
      Snapshot(rep._2.key, rep._2.valueOption, seq)
    }
    s.toVector
  }
  private def batch(snap: Snapshot): Vector[Snapshot] = {
    val idx = pending.indexWhere { snap.key == _.key }
    if (idx != -1) pending = pending.drop(idx)
    pending :+ snap
  }

}
