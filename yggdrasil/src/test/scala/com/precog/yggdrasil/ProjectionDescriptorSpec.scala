package com.precog.yggdrasil

import org.specs2.mutable.Specification

import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.json.serialization.DefaultSerialization._

import com.precog.common._
import com.precog.common.json._

import scala.collection.immutable.ListMap

import scalaz._

class ProjectionDescriptorSpec extends Specification {

  val descriptors = List(
    ColumnDescriptor(Path("/abc"), CPath(".foo.bar"), CString, Authorities(Set())),
    ColumnDescriptor(Path("/abc"), CPath(".foo.bar.baz"), CString, Authorities(Set())),
    ColumnDescriptor(Path("/def"), CPath(".bar.baz"), CLong, Authorities(Set()))
  )

  val testDescriptor = ProjectionDescriptor(3, descriptors)

  "ProjectionDescriptor" should {
    "serialize correctly" in {
      def roundTrip(in: ProjectionDescriptor): Validation[Extractor.Error, ProjectionDescriptor] = {
        def print = (_: JValue).renderPretty
        def parse = JParser.parse(_: String)

        val f = print andThen parse

        f(in.serialize).validated[ProjectionDescriptor]
      }

      roundTrip(testDescriptor) must beLike {
        case Success(pd) => 
          pd must_== testDescriptor 
      }
    }

  }
}
