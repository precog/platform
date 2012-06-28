package com.precog.yggdrasil

import table._
import com.precog.common.VectorCase
import blueeyes.json.JPath
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.JsonParser

import scalaz.{Ordering => _, NonEmptyList => NEL, _}
import scalaz.BiFunctor
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
  import trans.constants._

  override val defaultPrettyParams = Pretty.Params(2)

  def fromJson(sampleData: SampleData): Table
  def toJson(dataset: Table): Stream[JValue]

  def debugPrint(dataset: Table): Unit 

  def lookupF1(namespace: Vector[String], name: String): F1
  def lookupF2(namespace: Vector[String], name: String): F2

  implicit def keyOrder[A]: scala.math.Ordering[(Identities, A)] = tupledIdentitiesOrder[A](IdentitiesOrder).toScalaOrdering

  def toRecord(ids: VectorCase[Long], jv: JValue): JValue = {
    JObject(Nil).set(Key, JArray(ids.map(JInt(_)).toList)).set(Value, jv)
  }

  def sample(schema: Int => Gen[JSchema]) = Arbitrary(
    for {
      depth   <- choose(0, 3)
      jschema <- schema(depth)
      (idCount, data) <- genEventColumns(jschema)
    } yield {
      SampleData(
        data.sorted.toStream map { 
          case (ids, jv) => toRecord(ids, assemble(jv))
        },
        Some(jschema)
      )
    }
  )

  case class SampleData(data: Stream[JValue], schema: Option[JSchema] = None) {
    override def toString = {
      "\nSampleData: ndata = "+data.map(_.toString.replaceAll("\n", "\n  ")).mkString("[\n  ", ",\n  ", "]\n")
    }
  }

  def checkMappings = {
    implicit val gen = sample(schema)
    check { (sample: SampleData) =>
      val dataset = fromJson(sample)
      toJson(dataset).toList must containAllOf(sample.data.toList).only
    }
  }
}

// vim: set ts=4 sw=4 et:
