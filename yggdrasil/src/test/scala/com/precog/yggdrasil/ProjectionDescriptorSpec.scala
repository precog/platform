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

import org.specs2.mutable.Specification

import blueeyes.json.JPath
import blueeyes.json.Printer
import blueeyes.json.JsonParser
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._

import com.precog.analytics.Path
import com.precog.common._

import scala.collection.immutable.ListMap

import scalaz._

class ProjectionDescriptorSpec extends Specification {

  val descriptors = List(
    ColumnDescriptor(Path("/abc"), JPath(".foo.bar"), SStringArbitrary, Ownership(Set())),
    ColumnDescriptor(Path("/abc"), JPath(".foo.bar.baz"), SStringArbitrary, Ownership(Set())),
    ColumnDescriptor(Path("/def"), JPath(".bar.baz"), SLong, Ownership(Set())))

  val indexes = List(0, 0, 1)

  val columns = descriptors.zip(indexes).foldLeft(ListMap[ColumnDescriptor, Int]()){ (acc, el) => acc + el }

  val sorting = Seq((descriptors(0), ById), (descriptors(1), ByValue), (descriptors(0), ByValue))

  val pdValidation = ProjectionDescriptor(columns, sorting)

  val testDescriptor = pdValidation.toOption.get

  "ProjectionDescriptor" should {
    "serialize correctly" in {
      def roundTrip(in: ProjectionDescriptor): Validation[Extractor.Error, ProjectionDescriptor] = {
        def print = Printer.render _ andThen Printer.pretty _ 
        def parse = JsonParser.parse(_: String)

        val f = print andThen parse

        f(in.serialize).validated[ProjectionDescriptor]
      }

      roundTrip(testDescriptor) must beLike {
        case Success(pd) => pd must_== testDescriptor 
      }
    }

  }
}
