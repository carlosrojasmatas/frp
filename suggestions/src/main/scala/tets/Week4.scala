package tets

import rx.lang.scala.Subscription
import rx.lang.scala.subscriptions._
import rx.lang.scala.Observable
import rx.lang.scala.subjects.PublishSubject
import rx.lang.scala.subjects.ReplaySubject
import rx.lang.scala.subjects.AsyncSubject

trait Mathematics {
  type Number

  trait PrimeExtractor {
    def unapply(x: Number): Option[Number]
  }

  val Prime: PrimeExtractor
}
object Week4 {

  def main(args: Array[String]): Unit = {
    //    val s = Subscription {
    //      println("bye, bye")
    //    }
    //    s.unsubscribe()
    //    
    //    val a = Subscription(println("bye a"))
    //    val b = Subscription(println("bye b"))
    //    
    ////    val c = CompositeSubscription(a,b)
    //    
    //    val c = SerialSubscription()
    //    c.subscription = a
    //    c.subscription = b

    //    c.unsubscribe()
    //    println(c.isUnsubscribed)
    //    
    //    a.unsubscribe()
    //    println(c.isUnsubscribed)
    //    println(a.isUnsubscribed)
    //    println(b.isUnsubscribed)
    //
    //    import scala.language.postfixOps
    //    import scala.concurrent.duration._
    //
    //    val x: Observable[Long] = Observable.interval(1 seconds).take(5)
    //    val y = x.toBlocking.toList
    //    println(y)
    //
    //    val z = x.sum
    //
    //    val sum = z.toBlocking.single

    //    println(sum)

    //    val cnn = PublishSubject[Int]()
    //    val cnn = ReplaySubject[Int]()
    val cnn = AsyncSubject[Int]()
    val a = cnn.subscribe(x => println(s"a:$x"))
    val b = cnn.subscribe(x => println(s"b:$x"))

    cnn.onNext(42)

    a.unsubscribe()
    cnn.onNext(17)

    //    cnn.onCompleted()

    val c = cnn.subscribe(x => println(s"c:$x"))

    cnn.onNext(8)
    Thread.sleep(3000)

  }
}  