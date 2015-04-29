package quickcheck

import common._
import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._
import scala.language.implicitConversions

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  implicit def asList(h: H): List[Int] = {

    def toList(h: H): List[Int] = {
      if (isEmpty(h)) Nil else findMin(h) :: toList(deleteMin(h))
    }

    toList(h)
  }

  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  property("min2") = forAll { (a: Int, b: Int) =>
    val h = insert(a, empty)
    findMin(insert(b, h)) == (if (a < b) a else b)
  }

  property("min3") = forAll { a: Int =>
    val h = insert(a, empty)
    isEmpty(deleteMin(h))
  }

  property("gen1") = forAll { (h: H) =>
    val m = if (isEmpty(h)) 0 else findMin(h)
    findMin(insert(m, h)) == m
  }

  property("meld1") = forAll { h: (H, H) =>
    val h1 = h._1
    val h2 = h._2

    if (isEmpty(h1) && isEmpty(h2)) true
    else {
      val m = meld(h1, h2)
      findMin(m) == {
        val m1 = findMin(h1)
        val m2 = findMin(h2)
        if (m1 < m2) m1 else m2
      }
    }

  }

  property("meld2") = forAll { h: (H, H) =>
    val h1 = h._1
    val h2 = h._2

    if (isEmpty(h1) && isEmpty(h2)) true
    else {

      val l1: List[Int] = h1
      val l2: List[Int] = h2
      val m3:List[Int] = meld(h1,h2)
      m3 == (l1 ++ l2).sorted
    }

  }

  lazy val genHeap: Gen[H] = for {
    a <- arbitrary[Int]
    h <- oneOf[H](empty, genHeap)
  } yield insert(a, h)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

}
