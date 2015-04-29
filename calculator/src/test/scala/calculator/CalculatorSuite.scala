package calculator

import org.scalatest.FunSuite

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.scalatest._

import TweetLength.MaxTweetLength

@RunWith(classOf[JUnitRunner])
class CalculatorSuite extends FunSuite with ShouldMatchers {

  /**
   * ****************
   * * TWEET LENGTH **
   * ****************
   */

  def tweetLength(text: String): Int =
    text.codePointCount(0, text.length)

  test("tweetRemainingCharsCount with a constant signal") {
    val result = TweetLength.tweetRemainingCharsCount(Var("hello world"))
    assert(result() == MaxTweetLength - tweetLength("hello world"))

    val tooLong = "foo" * 200
    val result2 = TweetLength.tweetRemainingCharsCount(Var(tooLong))
    assert(result2() == MaxTweetLength - tweetLength(tooLong))
  }

  test("tweetRemainingCharsCount with a supplementary char") {
    val result = TweetLength.tweetRemainingCharsCount(Var("foo blabla \uD83D\uDCA9 bar"))
    assert(result() == MaxTweetLength - tweetLength("foo blabla \uD83D\uDCA9 bar"))
  }

  test("colorForRemainingCharsCount with a constant signal") {
    val resultGreen1 = TweetLength.colorForRemainingCharsCount(Var(52))
    assert(resultGreen1() == "green")
    val resultGreen2 = TweetLength.colorForRemainingCharsCount(Var(15))
    assert(resultGreen2() == "green")

    val resultOrange1 = TweetLength.colorForRemainingCharsCount(Var(12))
    assert(resultOrange1() == "orange")
    val resultOrange2 = TweetLength.colorForRemainingCharsCount(Var(0))
    assert(resultOrange2() == "orange")

    val resultRed1 = TweetLength.colorForRemainingCharsCount(Var(-1))
    assert(resultRed1() == "red")
    val resultRed2 = TweetLength.colorForRemainingCharsCount(Var(-5))
    assert(resultRed2() == "red")
  }

//  test("Calculator filter references") {
//
//    val origin = Map[String, Signal[Expr]](
//      "a" -> Signal(Plus(Ref("e"), Literal(2))),
//      "b" -> Signal(Plus(Ref("a"), Literal(2))),
//      "c" -> Signal(Plus(Ref("a"), Ref("b"))),
//      "d" -> Signal(Times(Literal(2), Literal(4))),
//      "e" -> Signal(Times(Literal(2), Literal(4))))
//
//    val rs = Calculator.getReferences("a", origin): Map[String, Signal[Expr]]
//    assert(rs.contains("e"))
//    assert(!rs.contains("d"))
//
//    val rs2 = Calculator.getReferences("b", origin): Map[String, Signal[Expr]]
//    assert(rs2.contains("a"))
//    assert(!rs2.contains("d"))
//
//    val rs3 = Calculator.getReferences("e", origin): Map[String, Signal[Expr]]
//    assert(rs3.size == 0)
//
//    val rs4 = Calculator.getReferences("c", origin): Map[String, Signal[Expr]]
//    assert(rs4.contains("a"))
//    assert(rs4.contains("b"))


//  }

  test("Calculator computing capability") {
    val origin = Map[String, Signal[Expr]](
      "a" -> Signal(Plus(Ref("e"), Literal(2))),
      "b" -> Signal(Plus(Ref("a"), Literal(2))),
      "c" -> Signal(Plus(Ref("a"), Ref("b"))),
      "d" -> Signal(Times(Literal(2), Literal(4))),
      "e" -> Signal(Times(Literal(2), Literal(4))))

    val rs = Calculator.computeValues(origin)

    assert(rs("a")() == 10)
    assert(rs("b")() == 12)
    assert(rs("c")() == 22)
    assert(rs("d")() == 8)
    assert(rs("e")() == 8)

    val ciclyc1 = Map[String, Signal[Expr]](
      "a" -> Signal(Plus(Ref("e"), Literal(2))),
      "b" -> Signal(Plus(Ref("a"), Literal(2))),
      "c" -> Signal(Plus(Ref("a"), Ref("b"))),
      "d" -> Signal(Times(Literal(2), Literal(4))),
      "e" -> Signal(Times(Ref("a"), Literal(4))))

    val rs2 = Calculator.computeValues(ciclyc1)
    assert(rs2("a")().equals(Double.NaN))
    assert(rs2("e")().equals(Double.NaN))

  }

}
