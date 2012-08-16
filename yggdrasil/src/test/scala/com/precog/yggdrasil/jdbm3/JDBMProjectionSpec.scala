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
import com.precog.common.json._

import java.io.File

class JDBMProjectionSpec extends Specification with ScalaCheck with Logging {
  import Gen._
  import Arbitrary._

  val maxArraySize = 3
  val maxArrayDepth = 2

  def genColumn(size: Int, values: Gen[Array[CValue]]): Gen[List[Seq[CValue]]] = containerOfN[List,Seq[CValue]](size, values.map(_.toSeq))

  def containerOfAtMostN[C[_],T](maxSize: Int, g: Gen[T])(implicit b: org.scalacheck.util.Buildable[T,C]): Gen[C[T]] =
    Gen.sized(size => for(n <- choose(0, size max maxSize); c <- containerOfN[C,T](n,g)) yield c)

  def indexedSeqOf[A](gen: Gen[A]): Gen[IndexedSeq[A]] = containerOfAtMostN[List, A](maxArraySize, gen) map (_.toIndexedSeq)

  def genValueFor[A](cType: CValueType[A]): Gen[A] = cType match {
    case CString => arbString.arbitrary
    case CBoolean => Gen.oneOf(true, false)
    case CLong => arbLong.arbitrary
    case CDouble => arbDouble.arbitrary
    case CNum => for {
      scale  <- arbInt.arbitrary
      bigInt <- arbBigInt.arbitrary
    } yield BigDecimal(new java.math.BigDecimal(bigInt.bigInteger, scale - 1), java.math.MathContext.UNLIMITED)
    case CDate =>
      choose[Long](0, Long.MaxValue) map (new DateTime(_))
    case cType @ CArrayType(elemType) =>
      indexedSeqOf(genValueFor(elemType))
  }

  def genFor(tpe: CType): Gen[CValue] = tpe match {
    case CString  => arbString.arbitrary.map(CString(_))
    case CBoolean => arbBool.arbitrary.map(CBoolean(_)) 
    case CLong    => arbLong.arbitrary.map(CLong(_))
    case CDouble  => arbDouble.arbitrary.map(CDouble(_))
    // ScalaCheck's arbBigDecimal fails on argument creation intermittently due to math context conflicts with scale/value.
    case CNum     => for {
      scale  <- arbInt.arbitrary
      bigInt <- arbBigInt.arbitrary
    } yield CNum(BigDecimal(new java.math.BigDecimal(bigInt.bigInteger, scale - 1 /* BigDecimal can't handle Integer min/max scales */), java.math.MathContext.UNLIMITED))
    case CDate    => arbLong.arbitrary.map { ts => CDate(new DateTime(ts)) }
    case cType @ CArrayType(_) =>
      genValueFor(cType) map { a => CArray(a, cType) }
    case CNull    => Gen.value(CNull)
    case CEmptyObject => Gen.value(CEmptyObject)
    case CEmptyArray  => Gen.value(CEmptyArray)
    case invalid      => sys.error("No values for type " + invalid)
  }

  def genNonArrayCValueType: Gen[CValueType[_]] = Gen.oneOf[CValueType[_]](CString, CBoolean, CLong, CDouble, CNum, CDate)
  def genCValueType(maxDepth: Int = maxArrayDepth, depth: Int = 0): Gen[CValueType[_]] = {
    if (depth >= maxDepth) genNonArrayCValueType else {
      frequency(1 -> (genCValueType(maxDepth, depth + 1) map (CArrayType(_))), 6 -> genNonArrayCValueType)
    }
  }

  def genCType: Gen[CType] = frequency(7 -> genCValueType(), 3 -> Gen.oneOf(CNull, CEmptyObject, CEmptyArray))

  override def defaultValues = super.defaultValues + (minTestsOk -> 20)

  case class ProjectionData(desc: ProjectionDescriptor, data: List[Seq[CValue]])

  implicit val genData: Arbitrary[ProjectionData] = Arbitrary(
    for {
      size       <- chooseNum(1,100000)
      width      <- chooseNum(1,40)
      types      <- listOfN(width, genCType)
      descriptor <- ProjectionDescriptor(1, types.toList.map { tpe => ColumnDescriptor(Path("/test"), CPath.Identity, tpe, Authorities(Set.empty)) })
      val typeGens: Seq[Gen[CValue]] = types.map(genFor)
      data       <- genColumn(size, sequence[Array, CValue](typeGens))
    } yield ProjectionData(descriptor, data)
  )

  def readWriteColumn(pd: ProjectionData, baseDir: File) = {
    val ProjectionData(descriptor: ProjectionDescriptor, dataRaw: List[Seq[CValue]]) = pd
    val data = dataRaw.toArray
    logger.info("Running projection read/write spec of size " + data.length)
    logger.debug("Projection data writes to " + baseDir)

    try {
      val proj = new JDBMProjection(baseDir, descriptor){}

      logger.debug("New projection open")

      // Insert all data
      (0 until data.length).foreach {
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

      read.size mustEqual data.length
    } catch {
      case t: Throwable => logger.error("Error reading projection data"); throw t
    }
  }

  "JDBMProjections" should {
    "properly serialize and deserialize arbitrary columns" in {
      check {
        pd: ProjectionData =>
          readWriteColumn(pd, Files.createTempDir())
      }
    }

    val indexGen = new java.util.Random()

    "properly serialize and deserialize columns with undefined values" in {
      check {
        pd: ProjectionData => {
          val holeyData = pd.data.map {
            rowData => {
              val newRow = rowData.toArray
              val replaceIdx = indexGen.nextInt(newRow.length)
              newRow(replaceIdx) = CUndefined
              newRow.toSeq
            }
          }
          readWriteColumn(pd.copy(data = holeyData), Files.createTempDir())
        }
      }
    }
  }
}
