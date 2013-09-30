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

import scala.util.Random._

import com.precog.common._
import com.precog.util.IOUtils
import com.precog.yggdrasil._

import blueeyes.json._

import spire.implicits._

import java.io.File

trait ClusteringTestSupport {

  case class GeneratedPointSet(points: Array[Array[Double]], centers: Array[Array[Double]])

  def genPoints(n: Int, dimension: Int, k: Int): GeneratedPointSet = {

    def genPoint(x: => Double): Array[Double] = Array.fill(dimension)(x)

    val s = math.pow(2 * k, 1.0 / dimension)
    val centers = (1 to k).map({ _ => genPoint(nextDouble * s) }).toArray
    val points = (1 to n).map({ _ =>
      val c = nextInt(k)
      genPoint(nextGaussian) + centers(c)
    }).toArray

    GeneratedPointSet(points, centers)
  }

  def pointToJson(p: Array[Double]): RValue = {
    RArray(p.toSeq map (CNum(_)): _*)
  }

  def pointsToJson(points: Array[Array[Double]]): List[RValue] = points.toList map (pointToJson(_))

  def writePointsToDataset[A](points: Array[Array[Double]])(f: String => A): A = {
    writeRValuesToDataset(pointsToJson(points))(f)
  }

  def writeRValuesToDataset[A](jvals: List[RValue])(f: String => A): A = {
    val lines = jvals map { _.toJValue.renderCompact }
    val tmpFile = File.createTempFile("values", ".json")
    IOUtils.writeSeqToFile(lines, tmpFile).unsafePerformIO
    val pointsString0 = "filesystem" + tmpFile.toString
    val pointsString = pointsString0.take(pointsString0.length - 5)
    val result = f(pointsString)
    tmpFile.delete()
    result
  }

}
