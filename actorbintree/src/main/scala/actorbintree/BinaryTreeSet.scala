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
      context.become(garbageCollecting(newRoot))
      root ! CopyTo(newRoot)
    }
  }

  // optional
  /**
   * Handles messages while garbage collection is performed.
   * `newRoot` is the root of the new binary tree where we want to copy
   * all non-removed elements into.
   */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case CopyFinished => {
      root ! PoisonPill
      root = newRoot
      pendingQueue foreach (root ! _)
      pendingQueue = Queue.empty[Operation]
      context.become(normal)
    }
    case o: Operation => pendingQueue = pendingQueue enqueue o
    case GC           =>
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
    //
    case con: Contains => {
      if (con.elem == elem) {
        con.requester ! ContainsResult(con.id, !removed)
      } else {

        if (con.elem > elem) {
          subtrees.get(Right) match {
            case Some(ref) => ref forward con
            case None => {
              con.requester ! ContainsResult(con.id, false)
            }
          }

        } else
          subtrees.get(Left) match {
            case Some(ref) => ref forward con
            case None => {
              con.requester ! ContainsResult(con.id, false)
            }
          }
      }
    }

    case in: Insert => {
      if (in.elem > elem) {
        subtrees.get(Right) match {
          case Some(ref) => ref ! in
          case None => {
            subtrees += Right -> context.actorOf(props(in.elem, false))
            in.requester ! OperationFinished(in.id)
          }
        }

      } else if (in.elem < elem)
        subtrees.get(Left) match {
          case Some(ref) => ref ! in
          case None => {
            subtrees += Left -> context.actorOf(props(in.elem, false))
            in.requester ! OperationFinished(in.id)

          }
        }
      else {

        if (removed) {
          removed = false
        }

        in.requester ! OperationFinished(in.id)
      }
    }

    //remove handler

    case rem: Remove => {
      if (rem.elem > elem) {
        subtrees.get(Right) match {
          case Some(ref) => ref forward rem
          case None => {
            rem.requester ! OperationFinished(rem.id)
          }
        }

      } else if (rem.elem < elem) {
        subtrees.get(Left) match {
          case Some(ref) => ref forward rem
          case None => {
            rem.requester ! OperationFinished(rem.id)
          }
        }
      } else {
        removed = true
        rem.requester ! OperationFinished(rem.id)
      }
    }

    case ct: CopyTo => {

      val expected = subtrees.map { case (k, v) => v }.toSet

      if (expected.isEmpty && removed) sender ! CopyFinished //empty leaf
      else {
        context.become(copying(expected, removed))
        expected.foreach(_ ! ct)
        if (!removed) ct.treeNode ! Insert(self, -1, elem)
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
        context.become(normal)
        context.parent ! CopyFinished
      } else {
        context.become(copying(expected, true))
      }
    }

    case CopyFinished => {

      if (expected.isEmpty) {
        if (insertConfirmed) {
          context.become(normal)
          context.parent ! CopyFinished
        }
      } else {
        val next = expected - expected.head
        if (next.isEmpty) { //no more waiting
          if (insertConfirmed) { // I'm done
            context.become(normal)
            context.parent ! CopyFinished
          } else {
            context.become(copying(next, insertConfirmed))
          }
        } else {
          context.become(copying(next, insertConfirmed))
        }
      }
    }

  }

}
