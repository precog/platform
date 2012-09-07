package com.precog.yggdrasil

import table._
import com.precog.common.VectorCase

import akka.dispatch.Future
import blueeyes.json.JPath
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.JsonParser
import blueeyes.json.xschema.DefaultOrderings.JValueOrdering
import blueeyes.concurrent.test.FutureMatchers

import scalaz.{Ordering => _, NonEmptyList => NEL, _}
import scalaz.std.tuple._
import scalaz.std.function._
import scalaz.syntax.arrow._
import scalaz.syntax.bifunctor._
import scalaz.syntax.copointed._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.generic.CanBuildFrom
import scala.util.Random

import org.specs2._
import org.specs2.mutable.Specification
import org.scalacheck._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._
import CValueGenerators.JSchema

trait TestLib[M[+_]] extends TableModule[M] {
  def lookupF1(namespace: List[String], name: String): F1 
  def lookupF2(namespace: List[String], name: String): F2
  def lookupScanner(namespace: List[String], name: String): Scanner 
}

trait TableModuleTestSupport[M[+_]] extends TableModule[M] with TestLib[M] {
  implicit def M: Monad[M] with Copointed[M]

  def fromJson(data: Stream[JValue], maxBlockSize: Option[Int] = None): Table
  def toJson(dataset: Table): M[Stream[JValue]] = dataset.toJson.map(_.toStream)

  def fromSample(sampleData: SampleData, maxBlockSize: Option[Int] = None): Table = fromJson(sampleData.data, maxBlockSize)

  def debugPrint(dataset: Table): Unit
}

trait TableModuleSpec[M[+_]] extends Specification with ScalaCheck {
  import SampleData._
  override val defaultPrettyParams = Pretty.Params(2)

  implicit def M: Monad[M] with Copointed[M]

  def checkMappings(testSupport: TableModuleTestSupport[M]) = {
    implicit val gen = sample(schema)
    check { (sample: SampleData) =>
      val dataset = testSupport.fromSample(sample)
      testSupport.toJson(dataset).copoint must containAllOf(sample.data.toList).only
    }
  }
}

// vim: set ts=4 sw=4 et:
