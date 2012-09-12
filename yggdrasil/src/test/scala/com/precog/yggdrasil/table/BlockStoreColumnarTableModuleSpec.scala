package com.precog.yggdrasil
package table

import com.precog.common.Path
import com.precog.common.VectorCase
import com.precog.bytecode.JType
import com.precog.yggdrasil.util._

import akka.actor.ActorSystem
import akka.dispatch._
import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import com.weiglewilczek.slf4s.Logging

import scala.annotation.tailrec
import scala.collection.BitSet
import scala.collection.mutable.LinkedHashSet
import scala.util.Random

import scalaz._
import scalaz.effect.IO 
import scalaz.syntax.copointed._
import scalaz.std.anyVal._

import org.specs2._
import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import org.scalacheck._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

import TableModule._
import TableModule.paths._

trait BlockStoreColumnarTableModuleSpec[M[+_]] extends TableModuleSpec[M] 
    with BlockLoadSpec[M]
    with BlockSortSpec[M] 
    with IntersectSpec[M]
    with BlockAlignSpec[M] 
    with GroupingSupportSpec[M]
    { self =>

  type MemoId = Int

  "a block store columnar table" should {
    "load" >> {
      "a problem sample" in testLoadSample1
      "a problem sample" in testLoadSample2
      "a problem sample" in testLoadSample3
      "a problem sample" in testLoadSample4
      //"a problem sample" in testLoadSample5 //pathological sample in the case of duplicated ids.
      "a dense dataset" in checkLoadDense
    }                           

    "sort" >> {
      "fully homogeneous data"        in homogeneousSortSample
      "data with undefined sort keys" in partiallyUndefinedSortSample
      "heterogeneous sort keys"       in heterogeneousSortSample
      "top-level hetereogeneous values" in heterogeneousBaseValueTypeSample
      "sort with a bad schema"        in badSchemaSortSample
      "merges over three cells"       in threeCellMerge
      "empty input"                   in emptySort
      "arbitrary datasets"            in checkSortDense
    }

    "align" >> {
      "a simple example" in alignSimple
      "across slice boundaries" in alignAcrossBoundaries
      "survive a trivial scalacheck" in checkAlign
    }

    "intersect by identity" >> {
      "simple data" in testSimpleIntersect
      "survive a trivial scalacheck" in checkIntersect
    }
  }
}

object BlockStoreColumnarTableModuleSpec extends BlockStoreColumnarTableModuleSpec[Free.Trampoline] {
  implicit def M = Trampoline.trampolineMonad

  type YggConfig = IdSourceConfig
  val yggConfig = new IdSourceConfig {
    val idSource = new IdSource {
      private val source = new java.util.concurrent.atomic.AtomicLong
      def nextId() = source.getAndIncrement
    }
  }
}

// vim: set ts=4 sw=4 et:
