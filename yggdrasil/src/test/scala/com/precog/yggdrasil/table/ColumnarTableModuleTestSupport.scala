package com.precog.yggdrasil
package table

import com.precog.common._
import com.precog.bytecode.JType
import com.precog.common.json._

import akka.actor.ActorSystem

import blueeyes.json._

import scala.annotation.tailrec

import scalaz._
import scalaz.syntax.copointed._
import scalaz.syntax.monad._
import scalaz.syntax.std.boolean._
import scalaz.std.anyVal._

import com.precog.util.{BitSetUtil, BitSet, Loop}
import com.precog.util.BitSetUtil.Implicits._

import TableModule._

trait ColumnarTableModuleTestSupport[M[+_]] extends ColumnarTableModule[M] with TableModuleTestSupport[M] {
  def newGroupId: GroupId

  def defaultSliceSize = 10

  def fromJson(values: Stream[JValue], maxSliceSize: Option[Int] = None): Table =
    Table.fromJson(values, maxSliceSize orElse Some(defaultSliceSize))

  def lookupF1(namespace: List[String], name: String): F1 = {
    val lib = Map[String, CF1](
      "negate" -> cf.math.Negate,
      "coerceToDouble" -> cf.util.CoerceToDouble,
      "true" -> CF1("testing::true") { _ => Some(Column.const(true)) }
    )

    lib(name)
  }

  def lookupF2(namespace: List[String], name: String): F2 = {
    val lib  = Map[String, CF2](
      "add" -> cf.math.Add,
      "mod" -> cf.math.Mod,
      "eq"  -> cf.std.Eq
    )
    lib(name)
  }

  def lookupScanner(namespace: List[String], name: String): CScanner = {
    val lib = Map[String, CScanner](
      "sum" -> new CScanner {
        type A = BigDecimal
        val init = BigDecimal(0)
        def scan(a: BigDecimal, cols: Map[ColumnRef, Column], range: Range): (A, Map[ColumnRef, Column]) = {
          val identityPath = cols collect { case c @ (ColumnRef(CPath.Identity, _), _) => c }
          val prioritized = identityPath.values filter {
            case (_: LongColumn | _: DoubleColumn | _: NumColumn) => true
            case _ => false
          }

          val mask = BitSetUtil.filteredRange(range.start, range.end) {
            i => prioritized exists { _ isDefinedAt i }
          }
          
          val (a2, arr) = mask.toList.foldLeft((a, new Array[BigDecimal](range.end))) {
            case ((acc, arr), i) => {
              val col = prioritized find { _ isDefinedAt i }
              
              val acc2 = col map {
                case lc: LongColumn =>
                  acc + lc(i)
                
                case dc: DoubleColumn =>
                  acc + dc(i)
                
                case nc: NumColumn =>
                  acc + nc(i)
              }
              
              acc2 foreach { arr(i) = _ }
              
              (acc2 getOrElse acc, arr)
            }
          }
          
          (a2, Map(ColumnRef(CPath.Identity, CNum) -> ArrayNumColumn(mask, arr)))
        }
      }
    )

    lib(name)
  }

  def debugPrint(dataset: Table): Unit = {
    println("\n\n")
    dataset.slices.foreach { slice => {
      M.point(for (i <- 0 until slice.size) println(slice.toString(i)))
    }}
  }
}


// vim: set ts=4 sw=4 et:
