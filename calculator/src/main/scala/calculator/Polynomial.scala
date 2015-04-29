package calculator

object Polynomial {
  def computeDelta(a: Signal[Double], b: Signal[Double],
                   c: Signal[Double]): Signal[Double] = {
    Signal(Math.pow(b(), 2) - 4 * a() * c())
  }

  def computeSolutions(a: Signal[Double], b: Signal[Double],
                       c: Signal[Double], delta: Signal[Double]): Signal[Set[Double]] = {
    Signal(getRoots(a(), b(), c(), delta()))
  }

  protected def getRoots(a: Double, b: Double, c: Double, delta: Double): Set[Double] = {
    if (delta == 0) Set()
    else {
      
      val x1part = -b + Math.sqrt(delta)
      val x2part = -b - Math.sqrt(delta)
      Set(x1part / (2 * a), x2part / (2 * a))
    }
  }
}
