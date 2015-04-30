package calculator

object Polynomial {
  
  
  def computeDelta(a: Signal[Double], b: Signal[Double],
                   c: Signal[Double]): Signal[Double] = {
    Signal(Math.pow(b(), 2) - (4 * a() * c()))
  }

  def computeSolutions(a: Signal[Double], b: Signal[Double],
                       c: Signal[Double], delta: Signal[Double]): Signal[Set[Double]] = {
    Signal(getRoots(a(), b(), c(), delta()))
  }

  protected def getRoots(a: Double, b: Double, c: Double, delta: Double): Set[Double] = {
    
    if (delta < 0) Set.empty
    
    else {
      
      val x1part = (-b + Math.sqrt(delta)) / (2 * a)
      val x2part = (-b - Math.sqrt(delta)) / (2 * a)
      Set(x1part, x2part)
    
    }
    
  }
}