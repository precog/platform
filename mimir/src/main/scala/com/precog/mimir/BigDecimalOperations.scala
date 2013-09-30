/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.mimir

object BigDecimalOperations {
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

  
