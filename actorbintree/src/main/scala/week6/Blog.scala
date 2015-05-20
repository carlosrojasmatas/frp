package week6

import akka.persistence.PersistentActor
import akka.actor.Actor
import akka.actor.ActorRef
import akka.persistence.AtLeastOnceDelivery
import akka.actor.ActorPath

object Blog {

  case class NewPost(text: String, id: Long)
  case class BlogPosted(id: Long)
  case class BlogNotPosted(id: Long, reason: String)
  case class PublishPost(text:String,id:Long)
  case class PostPublished(id:Long)

  sealed trait Event

  case class PostCreated(text: String) extends Event
  case object QuotaReached extends Event

  case class State(posts: Vector[String], disabled: Boolean) {
    def updated(e: Event): State = e match {
      case PostCreated(text) => copy(posts = posts :+ text)
      case QuotaReached      => copy(disabled = true)
    }
  }

  class UserProcessor(publisher:ActorPath) extends PersistentActor with AtLeastOnceDelivery {
    var state = State(Vector.empty, false)

    def receiveCommand = {
      case NewPost(text, id) =>
        // at-leat-once
        persist(PostCreated(text)){e =>
          deliver(publisher,PublishPost(text,_))  
        }
        
        //old fashion
        if(!state.disabled){
          val created = PostCreated(text)
          updateState(created)
          updateState(QuotaReached)
          persistAsync(created)(e => sender ! BlogPosted(id))
          persistAsync(QuotaReached)(_ => ())
        }else sender ! BlogNotPosted(id,"quota reached")
      case PostPublished(id) => confirmDelivery(id)
    }
    
    def updateState(e:Event){ state = state.updated(e)}
//    def receiveRecover = {case e : Event => updateState(e)}
    def receiveRecover = {
      case PostCreated(text) => deliver(publisher,PublishPost(text,_))
      case PostPublished(id) => confirmDelivery(id)
    }
  }
  
  class Publisher extends PersistentActor {
    var exp = 0L
    def persistenceId = ""
    def receiveCommand= {
      case PublishPost(text,id) =>
        if(id > exp) ()
        else if (id < exp) sender ! PostPublished(id)
        else persist(PostPublished(id)){ e =>
          sender ! e
          exp += 1
        }
    }
    
    def receiveRecover = { case PostPublished(id) => exp += 1}
  }

}