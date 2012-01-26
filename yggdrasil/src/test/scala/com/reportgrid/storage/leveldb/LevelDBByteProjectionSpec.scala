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
import org.scalacheck.{Arbitrary,Gen}
import org.specs2.ScalaCheck
import org.specs2.mutable.Specification

import scala.collection.immutable.ListMap

import com.reportgrid.yggdrasil.CValue
import com.reportgrid.yggdrasil.leveldb.LevelDBByteProjection
import com.reportgrid.yggdrasil.ProjectionDescriptor
import com.reportgrid.yggdrasil.ColumnDescriptor
import com.reportgrid.yggdrasil.SortBy
import com.reportgrid.yggdrasil.ById
import com.reportgrid.yggdrasil.ByValue
import com.reportgrid.yggdrasil.ByValueThenId
import com.reportgrid.yggdrasil.QualifiedSelector
import com.reportgrid.yggdrasil.ColumnType
import com.reportgrid.yggdrasil.SInt
import com.reportgrid.yggdrasil.SValue
import com.reportgrid.common.Metadata
import com.reportgrid.analytics.Path

import blueeyes.json.JPath

import scalaz._


case class CInt(value: Int) extends CValue {
  def fold[A](
    str:    String => A,
    bool:   Boolean => A,
    int:    Int => A,
    long:   Long => A,
    float:  Float => A,
    double: Double => A,
    num:    BigDecimal => A,
    emptyObj: => A,
    emptyArr: => A,
    nul:      => A
  ): A = int(value)
}


class LevelDBByteProjectionSpec extends Specification {
  "a byte projection" should {
    "project to the expected key format" in {
      val testIdentity: Vector[Long] = Vector(2L)
      val cv1 = CInt(5)
      val testValues: Seq[CValue] = Seq(cv1)

      val path: Path = Path("path")
      val selector: JPath = JPath("jpath")
      val valueType: ColumnType = SInt 

      val qsel: QualifiedSelector = QualifiedSelector(path, selector, valueType)
      val metadata: Set[Metadata] = Set()

      val listmap0: ColumnDescriptor = ColumnDescriptor(qsel, metadata)
      val int0: Int = 0 //must be 0 so that identity indexes are 0-based
      //val int1: Int = 1

      val columns: ListMap[ColumnDescriptor, Int] = ListMap(listmap0 -> int0)
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((listmap0, ByValue))
       
      val byteProjectionV = ProjectionDescriptor(columns, sorting) map { d => 
        new LevelDBByteProjection {
          val descriptor: ProjectionDescriptor = d
        }
      }

      val byteProjection = byteProjectionV ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) } 

      

      val expectedKey: Array[Byte] = Array(0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 2) 
      val expectedValue: Array[Byte] = Array()
      byteProjection.project(testIdentity, testValues)._1 must_== expectedKey
      byteProjection.project(testIdentity, testValues)._2 must_== expectedValue

    }
  }
}

