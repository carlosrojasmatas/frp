/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue
import akka.event.LoggingReceive

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /**
   * Request with identifier `id` to insert an element `elem` into the tree.
   * The actor at reference `requester` should be notified when this operation
   * is completed.
   */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /**
   * Request with identifier `id` to check whether an element `elem` is present
   * in the tree. The actor at reference `requester` should be notified when
   * this operation is completed.
   */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /**
   * Request with identifier `id` to remove the element `elem` from the tree.
   * The actor at reference `requester` should be notified when this operation
   * is completed.
   */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /**
   * Holds the answer to the Contains request with identifier `id`.
   * `result` is true if and only if the element is present in the tree.
   */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}

class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = LoggingReceive {
    case ops: Operation => root ! ops
    case GC => {
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      context.become(garbageCollecting(newRoot))
    }
  }

  // optional
  /**
   * Handles messages while garbage collection is performed.
   * `newRoot` is the root of the new binary tree where we want to copy
   * all non-removed elements into.
   */
  def garbageCollecting(newRoot: ActorRef): Receive = {

    case o: Operation => pendingQueue enqueue o

    case CopyFinished => {
      root ! PoisonPill
      root = newRoot
      pendingQueue foreach (root ! _)
      context.become(normal)
    }
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = LoggingReceive {

    case con: Contains if con.elem == elem => con.requester ! ContainsResult(con.id, !removed)
    case con: Contains if subtrees.isEmpty => con.requester ! ContainsResult(con.id, false)
    case con: Contains => {

      val r = subtrees.get(Right)
      val l = subtrees.get(Left)

      if (con.elem > elem) {
        r match {
          case Some(ref) => ref forward con
          case None => {
            con.requester ! ContainsResult(con.id, false)
          }
        }

      } else
        l match {
          case Some(ref) => ref forward con
          case None => {
            con.requester ! ContainsResult(con.id, false)
          }
        }
    }

    //insert handlers
    case in: Insert if subtrees.isEmpty => {
      if (in.elem == elem && removed) {
        removed = false
      }
      val ref = context.actorOf(props(in.elem, false))
      if (in.elem > elem) subtrees += Right -> ref
      else subtrees += Left -> ref
      in.requester ! OperationFinished(in.id)
    }
    case in: Insert => {
      if (in.elem == elem && removed) {
        removed = false
      }
      val r = subtrees.get(Right)
      val l = subtrees.get(Left)

      if (in.elem > elem) {
        r match {
          case Some(ref) => ref forward in
          case None => {
            subtrees += Right -> context.actorOf(props(in.elem, false))
            in.requester ! OperationFinished(in.id)
          }
        }

      } else
        l match {
          case Some(ref) => ref forward in
          case None => {
            subtrees += Left -> context.actorOf(props(in.elem, false))
            in.requester ! OperationFinished(in.id)

          }
        }
    }

    //remove handlers
    case rem: Remove if rem.elem == elem => {
      removed = true
      rem.requester ! OperationFinished(rem.id)
    }
    case rem: Remove if subtrees.isEmpty => rem.requester ! OperationFinished(rem.id)
    case rem: Remove => {

      val r = subtrees.get(Right)
      val l = subtrees.get(Left)

      if (rem.elem > elem) {
        r match {
          case Some(ref) => ref forward rem
          case None => {
            rem.requester ! OperationFinished(rem.id)
          }
        }

      } else
        l match {
          case Some(ref) => ref forward rem
          case None => {
            rem.requester ! OperationFinished(rem.id)
          }
        }

    }

    case ct: CopyTo => {

      val expected = subtrees.values.filter(_ != None).toSet
      println(s"Expected values ${expected.size}")

      if (expected.isEmpty && removed) sender ! CopyFinished

      else if (!expected.isEmpty && removed) {
        context.become(copying(expected, true))
        expected.foreach(_ forward ct)

      } else if (expected.isEmpty && !removed) {
        context.become(copying(expected, false))
        ct.treeNode ! Insert(self, elem, Integer.MAX_VALUE)
      }

    }
  }

  // optional
  /**
   * `expected` is the set of ActorRefs whose replies we are waiting for,
   * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
   */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {

    case of: OperationFinished => {
      if (expected.isEmpty) {
        context.parent ! CopyFinished
        context.become(normal)
      } else {
        context.become(copying(expected, true))
      }
    }

    case CopyFinished => {
      if (insertConfirmed) {
        context.parent ! CopyFinished
        context.become(normal)
      } else context.become(copying(expected - sender, insertConfirmed))
    }

    //    case of: OperationFinished if expected.isEmpty =>
    //      context.parent ! CopyFinished
    //      context.become(normal)
    //
    //    case OperationFinished =>
    //      context.become(copying(expected, true))
    //
    //    case CopyFinished if insertConfirmed =>
    //      context.parent ! CopyFinished
    //      context.become(normal)
    //
    //    case CopyFinished => {
    //      context.become(copying(expected - sender, insertConfirmed))
    //    }
  }

}
