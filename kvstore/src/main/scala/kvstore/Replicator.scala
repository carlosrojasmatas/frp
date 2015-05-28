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

  context.watch(replica)
  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  var waitingRoom = Map.empty[Long, Cancellable]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  context.system.scheduler.schedule(100 milliseconds, 100 milliseconds, self, Flush)
  //  context.system.scheduler.schedule(0 milliseconds, 100 milliseconds, self, Retry)

  /* TODO Behavior for the Replicator. */
  def receive: Receive = {

    case r @ Replicate(key, valueOption, id) => {
      val seq = nextSeq
      println(s"${self.path}: starting replication $id with sender ${sender.path} and seq $seq")
      acks = acks.updated(seq, (sender, r))
      println(acks)
      pending = batch(Replicator.Snapshot(key, valueOption, seq))
      waitingRoom += (seq -> context.system.scheduler.scheduleOnce(1 second, self, ReplicationFailed(seq)))
    }

    case ReplicationFailed(seq) =>
      println("failed")
      acks -= seq
      waitingRoom -= seq

    case SnapshotAck(key, seq) => {
      println(acks)
      println(s"ack received: $key from sender ${sender.path} with seq $seq")
      println(s"${acks.contains(seq)}")
      waitingRoom.get(seq).map(c => c.cancel())
      waitingRoom = waitingRoom - seq
      acks.get(seq) map (e => {
        println(s"sending ack to sender ${e._1.path}")
        e._1 ! Replicated(key, e._2.id)
      })

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
      seq <- waitingRoom.keySet
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
