package suggestions

import language.postfixOps
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{ Try, Success, Failure }
import rx.lang.scala._
import org.scalatest._
import gui._

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class WikipediaApiTest extends FunSuite {

  object mockApi extends WikipediaApi {
    def wikipediaSuggestion(term: String) = Future {
      if (term.head.isLetter) {
        for (suffix <- List(" (Computer Scientist)", " (Footballer)")) yield term + suffix
      } else {
        List(term)
      }
    }
    def wikipediaPage(term: String) = Future {
      "Title: " + term
    }
  }

  import mockApi._

  test("Sanitizer should replace white spaces with underscores") {
    val obs = Observable.from(List("hello world", "without white spaces")).sanitized

    val observed = scala.collection.mutable.Buffer[String]()
    val sub = obs subscribe {
      observed += _
    }

    assert(observed == Seq("hello_world", "without_white_spaces"), observed)

  }

  test("WikipediaApi should make the stream valid using sanitized") {
    val notvalid = Observable.just("erik", "erik meijer", "martin")
    val valid = notvalid.sanitized

    var count = 0
    var completed = false

    val sub = valid.subscribe(
      term => {
        assert(term.forall(_ != ' '))
        count += 1
      },
      t => assert(false, s"stream error $t"),
      () => completed = true)
    assert(completed && count == 3, "completed: " + completed + ", event count: " + count)
  }

  test("WikipediaApi should correctly use concatRecovered") {
    val requests = Observable.just(1, 2, 3)
    val remoteComputation = (n: Int) => Observable.just(0 to n: _*)
    val responses = requests concatRecovered remoteComputation
    val sum = responses.foldLeft(0) { (acc, tn) =>
      tn match {
        case Success(n) => acc + n
        case Failure(t) => throw t
      }
    }
    var total = -1
    val sub = sum.subscribe {
      s => total = s
    }
    assert(total == (1 + 1 + 2 + 1 + 2 + 3), s"Sum: $total")
  }

//  test("WikipediaApi should correctly use concatRecovered with errors") {
//    val requests = Observable.just(1, 2, 3, 4, 5)
//
//    val rs = requests.concatRecovered(n => if (n != 4) Observable.just(n) else Observable.error(new Error)).toBlocking.toList
//    assert(rs.toSeq === Seq(Success(1), Success(2), Success(3), Failure(new Error), Success(5)),rs)
//  }

  test("Observable(1, 2, 3).zip(Observable.interval(700 millis)).timedOut(1L) should return the first value, and complete without errors") {
    import scala.concurrent.duration.Duration._
    val o = Observable.just(1, 2, 3).zip(Observable.interval(700 millis)).timedOut(1L)
    var rs = List[Int]()

    val s = o.subscribe(
      n => rs = n._1 :: rs)
    Thread.sleep(1500)
    assert(rs.size == 1)
  }

}
