package com.precog.yggdrasil
package table

import com.precog.common._
import com.precog.common.security._
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

trait SliceColumnarTableModule[M[+_], Key] extends BlockStoreColumnarTableModule[M] with ProjectionModule[M, Key, Slice] with StorageMetadataSource[M] {
  type TableCompanion <: SliceColumnarTableCompanion

  trait SliceColumnarTableCompanion extends BlockStoreColumnarTableCompanion {
    type BD = BlockProjectionData[Key, Slice]
  
    private object loadMergeEngine extends MergeEngine[Key, BD]

    def load(table: Table, apiKey: APIKey, tpe: JType): M[Table] = {
      for {
        paths       <- pathsM(table)
        projections <- paths.toList.traverse(Projection(_)).map(_.flatten)
        totalLength = projections.map(_.length).sum
      } yield {
        def slices(proj: Projection, constraints: Option[Set[ColumnRef]]): StreamT[M, Slice] = {
          StreamT.unfoldM[M, Slice, Option[Key]](None) { key =>
            proj.getBlockAfter(key, constraints).map { b =>
              b.map {
                case BlockProjectionData(_, maxKey, slice) =>
                  (slice, Some(maxKey))
              }
            }
          }
        }

        val stream = projections.foldLeft(StreamT.empty[M, Slice]) {
          (acc, proj) =>
          // FIXME: Can Schema.flatten return Option[Set[ColumnRef]] instead?
          val constraints: M[Option[Set[ColumnRef]]] = proj.structure.map { struct =>
            Some(Schema.flatten(tpe, struct.toList).toSet)
          }

          acc ++ StreamT.wrapEffect(constraints map { c => slices(proj, c) })
        }
        Table(stream, ExactSize(totalLength))
      }
    }
  }
}
