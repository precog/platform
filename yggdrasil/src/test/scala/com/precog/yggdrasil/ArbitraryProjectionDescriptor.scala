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

import com.precog.common._
import com.precog.common.util._
import com.precog.common.json._
import blueeyes.json.JPath

import scala.collection.immutable.ListMap

import scalaz.Validation
import org.scalacheck.{Arbitrary, Gen}
import Gen._
import Arbitrary.arbitrary

trait ArbitraryProjectionDescriptor {
  def constructColumnDescriptor: Gen[ColumnDescriptor] = {
    def genPath: Gen[Path] = Gen.oneOf(Seq(Path("path1"),Path("path2"),Path("path3"),Path("path4"),Path("path5")))
    def genJPath: Gen[JPath] = Gen.oneOf(Seq(JPath("jpath1"),JPath("jpath2"),JPath("jpath3"),JPath("jpath4"),JPath("jpath5")))
    def genCType: Gen[CType] = Gen.oneOf(Seq(CBoolean, CLong, CDouble, CNum, CString))
    def genAuthorities: Gen[Authorities] = Gen.oneOf(Seq(Authorities(Set())))

    val genColumnDescriptor = for {
      path      <- genPath
      selector  <- genJPath // TODO genCPath
      valueType <- genCType
      ownership <- genAuthorities
    } yield ColumnDescriptor(path, CPath(selector), valueType, ownership)

    genColumnDescriptor
  }

  def genListColDes: Gen[List[ColumnDescriptor]] = for {
    number <- Gen.oneOf(1 to 30)
    listColDes <- Gen.listOfN(number, constructColumnDescriptor).map(_.groupBy(c => (c.path, c.selector)).filter {
      case (_, value) => value.size == 1 
     }.foldLeft(List.empty[ColumnDescriptor]) {
       case (list, (_, colDes)) => list ++ colDes
     })
  } yield {
    listColDes
  }
  
  def genProjectionDescriptor: Gen[ProjectionDescriptor] = for {
    identities <- Gen.oneOf(1 to 5)
    columnDescriptors <- genListColDes
  } yield {
    ProjectionDescriptor(identities, columnDescriptors)
  }
}


// vim: set ts=4 sw=4 et:
