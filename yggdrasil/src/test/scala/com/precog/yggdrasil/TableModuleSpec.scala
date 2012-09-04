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

case class SampleData(data: Stream[JValue], schema: Option[(Int, JSchema)] = None) {
  override def toString = {
    "SampleData: \ndata = "+data.map(_.toString.replaceAll("\n", "\n  ")).mkString("[\n  ", ",\n  ", "]\n") + 
    "\nschema: " + schema
  }
}

object SampleData extends CValueGenerators {
  def toRecord(ids: VectorCase[Long], jv: JValue): JValue = {
    JObject(Nil).set(JPath(".key"), JArray(ids.map(JNum(_)).toList)).set(JPath(".value"), jv)
  }

  implicit def keyOrder[A]: scala.math.Ordering[(Identities, A)] = tupledIdentitiesOrder[A](IdentitiesOrder).toScalaOrdering

  def sample(schema: Int => Gen[JSchema]) = Arbitrary(
    for {
      depth   <- choose(0, 1)
      jschema <- schema(depth)
      (idCount, data) <- genEventColumns(jschema)
    } yield {
      try {
      
      SampleData(
        data.sorted.toStream flatMap {
          // Sometimes the assembly process will generate overlapping values which will
          // cause RuntimeExceptions in JValue.unsafeInsert. It's easier to filter these
          // out here than prevent it from happening in the first place.
          case (ids, jv) => try { Some(toRecord(ids, assemble(jv))) } catch { case _ : RuntimeException => None }
        },
        Some((idCount, jschema))
      )
      } catch {
        case ex => println("depth: "+depth) ; throw ex
      }
    }
  )
  
  def distinctBy[T, C[X] <: Seq[X], S](c: C[T])(key: T => S)(implicit cbf: CanBuildFrom[C[T], T, C[T]]): C[T] = {
    val builder = cbf()
    val seen = mutable.HashSet[S]()
    
    for (t <- c) {
      if (!seen(key(t))) {
        builder += t
        seen += key(t)
      }
    }
    
    builder.result
  }
  
  def randomSubset[T, C[X] <: Seq[X], S](c: C[T], freq: Double)(implicit cbf: CanBuildFrom[C[T], T, C[T]]): C[T] = {
    val builder = cbf()
    
    for (t <- c)
      if (Random.nextDouble < freq)
        builder += t
    
    builder.result
  }
  
  def sort(sample: Arbitrary[SampleData]): Arbitrary[SampleData] = {
    Arbitrary(
      for {
        sampleData <- arbitrary(sample)
      } yield {
        SampleData(sampleData.data.sorted, sampleData.schema)
      }
    )
  }
  
  def shuffle(sample: Arbitrary[SampleData]): Arbitrary[SampleData] = {
    val gen =
      for {
        sampleData <- arbitrary(sample)
      } yield {
        SampleData(Random.shuffle(sampleData.data), sampleData.schema)
      }
    
    Arbitrary(gen)
  }

  def distinct(sample: Arbitrary[SampleData]) : Arbitrary[SampleData] = {
    Arbitrary(
      for {
        sampleData <- arbitrary(sample)
      } yield {
        SampleData(sampleData.data.distinct, sampleData.schema)
      }
    )
  }

  def distinctKeys(sample: Arbitrary[SampleData]) : Arbitrary[SampleData] = {
    Arbitrary(
      for {
        sampleData <- arbitrary(sample)
      } yield {
        SampleData(distinctBy(sampleData.data)(_ \ "keys"), sampleData.schema)
      }
    )
  }

  def distinctValues(sample: Arbitrary[SampleData]) : Arbitrary[SampleData] = {
    Arbitrary(
      for {
        sampleData <- arbitrary(sample)
      } yield {
        SampleData(distinctBy(sampleData.data)(_ \ "value"), sampleData.schema)
      }
    )
  }

  def duplicateRows(sample: Arbitrary[SampleData]): Arbitrary[SampleData] = {
    val gen =
      for {
        sampleData <- arbitrary(sample)
      } yield {
        val rows = sampleData.data
        val duplicates = randomSubset(rows, 0.25)
        SampleData(Random.shuffle(rows ++ duplicates), sampleData.schema)
      }
    
    Arbitrary(gen)
  }

  def undefineRows(sample: Arbitrary[SampleData]): Arbitrary[SampleData] = {
    val gen =
      for {
        sampleData <- arbitrary(sample)
      } yield {
        val rows = for(row <- sampleData.data) yield if(Random.nextDouble < 0.25) JNothing else row
        SampleData(rows, sampleData.schema) 
      }
    
    Arbitrary(gen)
  }

  def undefineRowsForColumn(sample: Arbitrary[SampleData], path: JPath): Arbitrary[SampleData] = {
    val gen =
      for {
        sampleData <- arbitrary(sample)
      } yield {
        val rows = for(row <- sampleData.data) yield if(Random.nextDouble < 0.25) row.set(path, JNothing)  else row
        SampleData(rows, sampleData.schema) 
      }
    
    Arbitrary(gen)
  }
}

trait TestLib[M[+_]] extends TableModule[M] {
  def lookupF1(namespace: List[String], name: String): F1 
  def lookupF2(namespace: List[String], name: String): F2
  def lookupScanner(namespace: List[String], name: String): Scanner 
}

trait TableModuleTestSupport[M[+_]] extends TableModule[M] {
  implicit def M: Monad[M] with Copointed[M]

  def fromJson(data: Stream[JValue], maxBlockSize: Option[Int] = None): Table
  def toJson(dataset: Table): M[Stream[JValue]] = dataset.toJson.map(_.toStream)

  def fromSample(sampleData: SampleData, maxBlockSize: Option[Int] = None): Table = fromJson(sampleData.data, maxBlockSize)

  def debugPrint(dataset: Table): Unit
}

trait TableModuleSpec[M[+_]] extends Specification with ScalaCheck with TableModuleTestSupport[M] with TestLib[M]{
  import SampleData._
  override val defaultPrettyParams = Pretty.Params(2)

  def checkMappings = {
    implicit val gen = sample(schema)
    check { (sample: SampleData) =>
      val dataset = fromSample(sample)
      toJson(dataset).copoint must containAllOf(sample.data.toList).only
    }
  }
}

// vim: set ts=4 sw=4 et:
