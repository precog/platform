package com.precog.yggdrasil

import table._
import com.precog.common.VectorCase
import blueeyes.json.JPath
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.JsonParser

import scalaz.{NonEmptyList => NEL, _}
import scalaz.BiFunctor
import scalaz.Ordering._
import scalaz.Either3._
import scalaz.std.tuple._
import scalaz.std.function._
import scalaz.syntax.arrow._
import scalaz.syntax.biFunctor._
import scala.annotation.tailrec

import org.specs2._
import org.specs2.mutable.Specification
import org.scalacheck._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

trait TableModuleSpec extends Specification with ScalaCheck with CValueGenerators with TableModule {
  type Record[A] = (Identities, A)

  override val defaultPrettyParams = Pretty.Params(2)

  implicit def order[A] = tupledIdentitiesOrder[A]()

  case class SampleData(idCount: Int, data: Stream[Record[JValue]]) {
    override def toString = {
      "\nSampleData: \nidCount = "+idCount+",\ndata = "+
      data.map({ case (ids, v) => ids.mkString("(", ",", ")") + ": " + v.toString.replaceAll("\n", "\n  ") }).mkString("[\n  ", ",\n  ", "]\n")
    }
  }

  def fromJson(sampleData: SampleData): Table
  def toJson(dataset: Table): Stream[Record[JValue]]

  def toValidatedJson(dataset: Table): Stream[Record[ValidationNEL[Throwable, JValue]]]
  def debugPrint(dataset: Table): Unit 

  def normalizeValidations(s: Stream[Record[ValidationNEL[Throwable, JValue]]]): Stream[Record[Option[JValue]]] = {
    s map {
      case (ids, Failure(t)) => t.list.foreach(_.printStackTrace); (ids, None)
      case (ids, Success(v)) => (ids, Some(v))
    }
  }

  implicit def identitiesOrdering = IdentitiesOrder.toScalaOrdering

  implicit val arbData = Arbitrary(
    for {
      depth   <- choose(0, 3)
      jschema <- schema(depth)
      (idCount, data) <- genEventColumns(jschema)
    } yield {
      SampleData(idCount, data.sortBy(_._1).toStream map { (assemble _).second })
    }
  )

  def checkMappings = {
    check { (sample: SampleData) =>
      val dataset = fromJson(sample)
      toJson(dataset).toList must containAllOf(sample.data.toList).only
    }
  }
}

// vim: set ts=4 sw=4 et:
