package kvstore

import akka.actor.Actor
import akka.actor.ActorRef
import kvstore.Replicator.Replicate
import kvstore.Replicator.Replicated
import kvstore.Persistence.Persisted

object ReplicaManager {
  case class Start(replicate: Replicate)
  case class Done(key: String, id: Long)
}
class ReplicaManager(replicators: Set[ActorRef],persistence:ActorRef) extends Actor {
  import ReplicaManager._

  def receive: Receive = {
    case Start(replicate) =>
      replicators foreach (_ ! replicate)
      context.become(this.replicate(replicators.size,false))
  }

  def replicate(pending: Int,persisted:Boolean): Receive = {

    case r @ Replicated(key, id) =>
      {
        if (pending == 0 && persisted) {
          context.parent ! Done(key,id)
          context.stop(self)
        } else {
          if(pending == 0)
          context.become(this.replicate(pending,persisted))
          else context.become(this.replicate(pending-1,persisted))
        }
      }
      
    case p@Persisted(key,id)=> {
      if(pending == 0){
    	  context.parent ! Done(key,id)
    	  context.stop(self)
      }else{
        context.become(this.replicate(pending,true))
      }
    }
  }

}