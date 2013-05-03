package com.precog.yggdrasil
package table

import com.precog.common._
import com.precog.bytecode.JType

import com.precog.util._
import com.precog.util.{BitSetUtil, BitSet, Loop}
import com.precog.util.BitSetUtil.Implicits._

import akka.actor.ActorSystem

import blueeyes.json._

import scala.annotation.tailrec

import scalaz._
import scalaz.std.anyVal._
import scalaz.syntax.monad._
import scalaz.syntax.std.boolean._


import TableModule._

trait ColumnarTableModuleTestSupport[M[+_]] extends ColumnarTableModule[M] with TableModuleTestSupport[M] {
  def newGroupId: GroupId

  def defaultSliceSize = 10

  private def makeSlice(sampleData: Stream[JValue], sliceSize: Int): (Slice, Stream[JValue]) = {
    @tailrec def buildColArrays(from: Stream[JValue], into: Map[ColumnRef, ArrayColumn[_]], sliceIndex: Int): (Map[ColumnRef, ArrayColumn[_]], Int) = {
      from match {
        case jv #:: xs =>
          val refs = Slice.withIdsAndValues(jv, into, sliceIndex, sliceSize)
          buildColArrays(xs, refs, sliceIndex + 1)
        case _ =>
          (into, sliceIndex)
      }
    }

    val (prefix, suffix) = sampleData.splitAt(sliceSize)
    val slice = new Slice {
      val (columns, size) = buildColArrays(prefix.toStream, Map.empty[ColumnRef, ArrayColumn[_]], 0) 
    }

    (slice, suffix)
  }
  
  // production-path code uses fromRValues, but all the tests use fromJson
  // this will need to be changed when our tests support non-json such as CDate and CPeriod
  def fromJson0(values: Stream[JValue], maxSliceSize: Option[Int] = None): Table = {
    val sliceSize = maxSliceSize.getOrElse(yggConfig.maxSliceSize)
  
    Table(
      StreamT.unfoldM(values) { events =>
        M.point {
          (!events.isEmpty) option {
            makeSlice(events.toStream, sliceSize)
          } 
        }
      },
      ExactSize(values.length)
    )
  }

  def fromJson(values: Stream[JValue], maxSliceSize: Option[Int] = None): Table =
    fromJson0(values, maxSliceSize orElse Some(defaultSliceSize))

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

  def lookupScanner(namespace: List[String], name: String): CScanner[M] = {
    val lib = Map[String, CScanner[M]](
      "sum" -> new CScanner[M] {
        type A = BigDecimal
        val init = BigDecimal(0)
        def scan(a: BigDecimal, cols: Map[ColumnRef, Column], range: Range): M[(A, Map[ColumnRef, Column])] = {
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
          
          M point (a2, Map(ColumnRef(CPath.Identity, CNum) -> ArrayNumColumn(mask, arr)))
        }
      }
    )

    lib(name)
  }
}


// vim: set ts=4 sw=4 et:
