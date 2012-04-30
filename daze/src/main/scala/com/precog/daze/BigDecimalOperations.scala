package com.precog.daze

trait BigDecimalOperations { 
  /**
   * Newton's approximation to some number of iterations (by default: 50).
   * Ported from a Java example found here: http://www.java2s.com/Code/Java/Language-Basics/DemonstrationofhighprecisionarithmeticwiththeBigDoubleclass.htm
   */

  def sqrt(d: BigDecimal, k: Int = 50): BigDecimal = {
    if (d > 0) {
      lazy val approx = {   // could do this with a self map, but it would be much slower
        def gen(x: BigDecimal): Stream[BigDecimal] = {
          val x2 = (d + x * x) / (x * 2)
          
          lazy val tail = if (x2 == x)
            Stream.empty
          else
            gen(x2)
          
          x2 #:: tail
        }
        
        gen(d / 3)
      }
      
      approx take k last
    } else if (d == 0) {
      0
    } else {
      sys.error("square root of a negative number")
    }
  }
}

  
