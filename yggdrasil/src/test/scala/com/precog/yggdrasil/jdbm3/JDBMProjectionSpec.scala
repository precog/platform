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
package com.precog.yggdrasil
package jdbm3

import com.google.common.io.Files
import com.weiglewilczek.slf4s.Logging

import org.apache.commons.io.FileUtils
import org.joda.time.DateTime
import org.specs2._
import org.specs2.mutable.Specification
import org.scalacheck.{Arbitrary,Gen}

import blueeyes.json.JPath

import com.precog.common.{Path,VectorCase}

class JDBMProjectionSpec extends Specification with ScalaCheck with Logging {
  import Gen._
  import Arbitrary._

  def genColumn(size: Int, values: Gen[Array[CValue]]): Gen[List[Seq[CValue]]] = containerOfN[List,Seq[CValue]](size, values.map(_.toSeq))

  def genFor(tpe: CType): Gen[CValue] = tpe match {
    case CString  => arbString.arbitrary.map(CString(_))
    case CBoolean => arbBool.arbitrary.map(CBoolean(_)) 
    case CLong    => arbLong.arbitrary.map(CLong(_))
    case CDouble  => arbDouble.arbitrary.map(CDouble(_))
    /* I don't care if Paul Phillips thinks it was a great idea, changing the default MathContext 
     * of scala.math.BigDecimal IS A HUGE MISTAKE AND PAIN IN THE REAR. Using Arbitrary.arbBigDecimal
     * will result in Overflow exceptions because it generates java.math.BigDecimals in MathContext.UNLIMITED,
     * but scala now defaults to MathContext.DECIMAL128 :(
     *
     * Submitting a patch to Ricky */
    case CNum     => for {
      scale  <- arbInt.arbitrary
      bigInt <- arbBigInt.arbitrary
    } yield CNum(BigDecimal(new java.math.BigDecimal(bigInt.bigInteger, scale - 1 /* BigDecimal can't handle Integer min/max scales */), java.math.MathContext.UNLIMITED))
    case CDate    => arbLong.arbitrary.map { ts => CDate(new DateTime(ts)) }
    case CNull    => Gen.value(CNull)
    case CEmptyObject => Gen.value(CEmptyObject)
    case CEmptyArray  => Gen.value(CEmptyArray)
  }
  
  override def defaultValues = super.defaultValues + (minTestsOk -> 20)

  "JDBMProjections" should {
    "properly serialize and deserialize arbitrary columns" in {
      case class ProjectionData(desc: ProjectionDescriptor, data: List[Seq[CValue]])

      implicit val genData: Arbitrary[ProjectionData] = Arbitrary(
        for {
          size       <- chooseNum(1,100000)
          width      <- chooseNum(1,20)
          types      <- pick(width, List(CString, CBoolean, CLong, CDouble, CNum , CDate, CNull, CEmptyObject, CEmptyArray))
          descriptor <- ProjectionDescriptor(1, types.toList.map { tpe => ColumnDescriptor(Path("/test"), JPath.Identity, tpe, Authorities(Set.empty)) })
          val typeGens: Seq[Gen[CValue]] = types.map(genFor)
          data       <- genColumn(size, sequence[Array, CValue](typeGens))
        } yield ProjectionData(descriptor, data)
      )

      check {
        pd: ProjectionData => {
          val ProjectionData(descriptor: ProjectionDescriptor, dataRaw: List[Seq[CValue]]) = pd
          val data = dataRaw.toArray
          val size = data.length
          val baseDir = Files.createTempDir()

          try {
            val proj = new JDBMProjection(baseDir, descriptor){}

            // Insert all data
            (0 until size).foreach {
              i => proj.insert(VectorCase(i), data(i), i % 100000 == 0).unsafePerformIO
            }
            
            proj.close()
          } catch {
            case t: Throwable => logger.error("Error writing projection data", t); throw t
          }

          try {
            val proj2 = new JDBMProjection(baseDir, descriptor){}

            val read = proj2.allRecords(Long.MaxValue).iterable.iterator.toList

            proj2.close()

            FileUtils.deleteDirectory(baseDir)

            forall(read.zipWithIndex) {
              case ((ids, v), i) => {
                ids mustEqual VectorCase(i)
                v   mustEqual data(i)
              }
            }

            read.size mustEqual size
          } catch {
            case t: Throwable => logger.error("Error reading projection data"); throw t
          }
        }
      }
    }
  }
}
