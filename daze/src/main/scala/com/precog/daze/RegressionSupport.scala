package com.precog
package daze

trait RegressionSupport {
  val Stats2Namespace = Vector("std", "stats")

  def dotProduct(xs: Array[Double], ys: Array[Double]): Double = {
    assert(xs.length == ys.length)
    var i = 0
    var result = 0.0
    while (i < xs.length) {
      result += xs(i) * ys(i)
      i += 1
    }
    result
  }    

  def arraySum(xs: Array[Double], ys: Array[Double]): Array[Double] = {
    assert(xs.length == ys.length)
    var i = 0
    var result = new Array[Double](xs.length)
    while (i < xs.length) {
      result(i) = xs(i) + ys(i)
      i += 1
    }
    result
  }    
}

