package com.precog.daze

import scala.util.Random
import scala.collection.mutable

import com.precog.yggdrasil._
import com.precog.yggdrasil.util.CPathUtils._
import com.precog.common.json._

import blueeyes.json._

trait RegressionTestSupport[M[+_]] {
  def makeT: Double = {
    val theta = Random.nextGaussian * 10
    if (theta == 0) makeT
    else theta
  }

  def makeThetas(length: Int): Array[Double] = {
    Seq.fill(length)(makeT).toArray
  }

  def jvalues(samples: Seq[(Array[Double], Double)], cpaths: Seq[CPath], mod: Int = 1): Seq[JValue] = samples.zipWithIndex map { case ((xs, y), idx) => 
    val cvalues = xs.map { x => CDouble(x).asInstanceOf[CValue] } :+ CDouble(y.toDouble).asInstanceOf[CValue] 
    val withCPath = {
      if (idx % mod == 0) cpaths zip cvalues.toSeq
      else if (idx % mod == 1) cpaths.tail zip cvalues.tail.toSeq
      else cpaths.tail.tail zip cvalues.tail.tail.toSeq
    }
    val withJPath = withCPath map { case (cpath, cvalue) => cPathToJPaths(cpath, cvalue) head }  // `head` is only okay if we don't have any homogeneous arrays
    val withJValue = withJPath map { case (jpath, cvalue) => (jpath, cvalue.toJValue) }
    withJValue.foldLeft(RArray(Nil).asInstanceOf[JValue]) { case (target, (jpath, jvalue)) => target.unsafeInsert(jpath, jvalue) }
  } 

  def stdDevMean(values: List[Double]): (Double, Double) = {
    val count = values.size
    val sum = values.sum
    val sumsq = values map { x => math.pow(x, 2) } sum

    val stdDev = math.sqrt(count * sumsq - sum * sum) / count
    val mean = sum / count

    (stdDev, mean)
  }

  //more robust way to deal with outliers than stdDev
  //the `constant` is the conversion constant to the units of standard deviation
  def madMedian(values: List[Double]): (Double, Double) = {
    val constant = 0.6745

    val sorted = values.sorted
    val length = sorted.length
    val median = sorted(length / 2)

    val diffs = values map { v => math.abs(v - median) }
    val sortedDiffs = diffs.sorted
    val mad = sortedDiffs(length / 2) / constant

    (mad, median) 
  }

  def combineResults(num: Int, thetas: List[List[Double]]) = {
    thetas.foldLeft(mutable.Seq.fill(num)(List.empty[Double])) { case (acc, li) => 
      var i = 0
      while (i < li.length) {
        acc(i) = acc(i) :+ li(i)
        i += 1
      }
      acc
    }
  }

  def isOk(actual: Double, computed: List[Double]): Boolean = {  
    val (mad, median) = madMedian(computed)
    val diff = math.abs(median - actual)

    diff < mad * 3D
  }
}

