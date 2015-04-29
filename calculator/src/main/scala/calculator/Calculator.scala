package calculator

sealed abstract class Expr
final case class Literal(v: Double) extends Expr
final case class Ref(name: String) extends Expr
final case class Plus(a: Expr, b: Expr) extends Expr
final case class Minus(a: Expr, b: Expr) extends Expr
final case class Times(a: Expr, b: Expr) extends Expr
final case class Divide(a: Expr, b: Expr) extends Expr

object Calculator {
  
  private var originalMap:Map[String,Signal[Expr]] = _

  def computeValues(
    namedExpressions: Map[String, Signal[Expr]]): Map[String, Signal[Double]] = {

    originalMap = namedExpressions
    namedExpressions.map {

      case (name, expr) => {

        val references = getReferences(name)
        
        if (findCycle(name, references)) {
          originalMap -= name
          (name, Signal(Double.NaN))
        }
        else 
          (name, Signal(eval(expr(), references)))

      }
    }
  }

  def findCycle(name: String, references: Map[String, Signal[Expr]]): Boolean = {
    
    val cycle = references.filter {
      case (k, v) => Calculator.getReferences(k).contains(name)
    }

    !cycle.isEmpty
  }

  def getReferences(refName:String): Map[String, Signal[Expr]] = {

    def dereference(exp: Expr,curr:List[String]): List[String] = {
      exp match {
        case Ref(n) => n :: curr
        case Literal(a)   => curr
        case Plus(a, b)   => dereference(a,curr) ::: dereference(b,curr) ::: curr
        case Minus(a, b)  => dereference(a,curr) ::: dereference(b,curr) ::: curr
        case Times(a, b)  => dereference(a,curr) ::: dereference(b,curr) ::: curr
        case Divide(a, b) => dereference(a,curr) ::: dereference(b,curr) ::: curr
      }
    }
    
    Map(dereference(getReferenceExpr(refName, originalMap),List()).map ( ref => (ref ,Signal(getReferenceExpr(ref, originalMap))) ) : _*)
  }

  def eval(expr: Expr, references: Map[String, Signal[Expr]]): Double = {
    expr match {
      case Ref(n) => {
        val refExp = getReferenceExpr(n, references)
        eval(refExp, getReferences(n))
      }
      case Literal(v)   => v
      case Plus(a, b)   => eval(a, references) + eval(b, references)
      case Minus(a, b)  => eval(a, references) - eval(b, references)
      case Times(a, b)  => eval(a, references) * eval(b, references)
      case Divide(a, b) => eval(a, references) / eval(b, references)
    }
  }

  /**
   * Get the Expr for a referenced variables.
   *  If the variable is not known, returns a literal NaN.
   */
  private def getReferenceExpr(name: String,
                               references: Map[String, Signal[Expr]]) = {

    references.get(name).fold[Expr] {
      Literal(Double.NaN)
    } { exprSignal =>
      exprSignal()
    }
  }
}
