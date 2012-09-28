package com.precog.yggdrasil

import org.specs2.mutable.Specification

import blueeyes.json.JPath
import blueeyes.json.Printer
import blueeyes.json.JsonParser
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._

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

  val pdValidation = ProjectionDescriptor(3, descriptors)

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
