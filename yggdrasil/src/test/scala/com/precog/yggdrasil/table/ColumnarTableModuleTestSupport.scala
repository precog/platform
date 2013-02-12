/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.yggdrasil
package table

import com.precog.bytecode.JType
import com.precog.common.Path
import com.precog.common.VectorCase
import com.precog.common.json._
import com.precog.util._

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

  /**
   * Given a JValue, an existing map of columnrefs to column data,
   * a sliceIndex, and a sliceSize, return an updated map.
   */
  def updateRefs(jv: JValue, into: Map[ColumnRef, ArrayColumn[_]], sliceIndex: Int, sliceSize: Int, remapPath: Option[JPath => CPath] = None): Map[ColumnRef, ArrayColumn[_]] = {

    jv.flattenWithPath.foldLeft(into) {
      case (acc, (jpath, JUndefined)) => acc
      case (acc, (jpath, v)) =>
        val ctype = CType.forJValue(v) getOrElse { sys.error("Cannot determine ctype for " + v + " at " + jpath + " in " + jv) }
        val ref = ColumnRef(remapPath.map(_(jpath)).getOrElse(CPath(jpath)), ctype)
        
        val updatedColumn: ArrayColumn[_] = v match {
          case JBool(b) =>
            acc.getOrElse(ref, ArrayBoolColumn.empty()).asInstanceOf[ArrayBoolColumn].tap { c => c.update(sliceIndex, b) }
            
          case JNum(d) => ctype match {
            case CLong =>
              acc.getOrElse(ref, ArrayLongColumn.empty(sliceSize)).asInstanceOf[ArrayLongColumn].tap { c => c.update(sliceIndex, d.toLong) }

            case CDouble =>
              acc.getOrElse(ref, ArrayDoubleColumn.empty(sliceSize)).asInstanceOf[ArrayDoubleColumn].tap { c => c.update(sliceIndex, d.toDouble) }

            case CNum =>
              acc.getOrElse(ref, ArrayNumColumn.empty(sliceSize)).asInstanceOf[ArrayNumColumn].tap { c => c.update(sliceIndex, d) }

            case _ => sys.error("non-numeric type reached")
          }
            
          case JString(s) =>
            acc.getOrElse(ref, ArrayStrColumn.empty(sliceSize)).asInstanceOf[ArrayStrColumn].tap { c => c.update(sliceIndex, s) }
            
          case JArray(Nil) =>
            acc.getOrElse(ref, MutableEmptyArrayColumn.empty()).asInstanceOf[MutableEmptyArrayColumn].tap { c => c.update(sliceIndex, true) }
            
          case JObject.empty =>
            acc.getOrElse(ref, MutableEmptyObjectColumn.empty()).asInstanceOf[MutableEmptyObjectColumn].tap { c => c.update(sliceIndex, true) }
            
          case JNull        =>
            acc.getOrElse(ref, MutableNullColumn.empty()).asInstanceOf[MutableNullColumn].tap { c => c.update(sliceIndex, true) }

          case _ => sys.error("non-flattened value reached")
        }
        
        acc + (ref -> updatedColumn)
    }
  }

  // production-path code uses fromRValues, but all the tests use fromJson
  // this will need to be changed when our tests support non-json such as CDate and CPeriod
  def fromJson0(values: Stream[JValue], maxSliceSize: Option[Int] = None): Table = {
    val sliceSize = maxSliceSize.getOrElse(yggConfig.maxSliceSize)
  
    def makeSlice(sampleData: Stream[JValue]): (Slice, Stream[JValue]) = {
      val (prefix, suffix) = sampleData.splitAt(sliceSize)
  
      @tailrec def buildColArrays(from: Stream[JValue], into: Map[ColumnRef, ArrayColumn[_]], sliceIndex: Int): (Map[ColumnRef, ArrayColumn[_]], Int) = {
        from match {
          case jv #:: xs =>
            val refs = updateRefs(jv, into, sliceIndex, sliceSize)
            buildColArrays(xs, refs, sliceIndex + 1)
          case _ =>
            (into, sliceIndex)
        }
      }
  
      // FIXME: If prefix is empty (eg. because sampleData.data is empty) the generated
      // columns won't satisfy sampleData.schema. This will cause the subsumption test in
      // Slice#typed to fail unless it allows for vacuous success
      val slice = new Slice {
        val (columns, size) = buildColArrays(prefix.toStream, Map.empty[ColumnRef, ArrayColumn[_]], 0) 
      }
  
      (slice, suffix)
    }
    
    Table(
      StreamT.unfoldM(values) { events =>
        M.point {
          (!events.isEmpty) option {
            makeSlice(events.toStream)
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
