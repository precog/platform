package com.precog.yggdrasil
package table
package mongo

import com.precog.common.{MetadataStats,Path,VectorCase}
import com.precog.common.json._
import com.precog.bytecode._
import com.precog.yggdrasil.jdbm3._
import com.precog.yggdrasil.util._
import com.precog.util._
import Schema._
import metadata._

import com.precog.util.{BitSet, BitSetUtil, Loop}
import com.precog.util.BitSetUtil.Implicits._

import java.io.File
import java.util.SortedMap
import java.util.Comparator

import org.apache.jdbm.DBMaker
import org.apache.jdbm.DB

import org.slf4j.LoggerFactory

import scalaz._
import scalaz.Ordering._
import scalaz.std.set._
import scalaz.std.list._
import scalaz.std.stream._
import scalaz.syntax.monad._
import scalaz.syntax.monoid._
import scalaz.syntax.traverse._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.stream._
import scala.annotation.tailrec
import scala.collection.mutable

import TableModule._

trait MongoColumnarTableModule extends BlockStoreColumnarTableModule[Future] {

  trait MongoColumnarTableCompanion extends BlockStoreColumnarTableCompanion {
    def load(table: Table, uid: UserId, tpe: JType): M[Table] = {
    }
  }
}


// vim: set ts=4 sw=4 et:
