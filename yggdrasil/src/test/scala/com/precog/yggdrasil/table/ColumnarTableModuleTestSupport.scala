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

import akka.actor.ActorSystem

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.JsonParser

import scala.annotation.tailrec
import scala.collection.BitSet

import scalaz._
import scalaz.syntax.copointed._
import scalaz.syntax.monad._
import scalaz.syntax.std.boolean._
import scalaz.std.anyVal._

import TableModule._

trait ColumnarTableModuleTestSupport[M[+_]] extends TableModuleTestSupport[M] with ColumnarTableModule[M] {
  def newGroupId: GroupId

  def fromJson(values: Stream[JValue], maxSliceSize: Option[Int] = None): Table = {
    val sliceSize = maxSliceSize.getOrElse(10)

    def makeSlice(sampleData: Stream[JValue]): (Slice, Stream[JValue]) = {
      val (prefix, suffix) = sampleData.splitAt(sliceSize)

      @tailrec def buildColArrays(from: Stream[JValue], into: Map[ColumnRef, (BitSet, Array[_])], sliceIndex: Int): (Map[ColumnRef, (BitSet, Object)], Int) = {
        from match {
          case jv #:: xs =>
            val withIdsAndValues = jv.flattenWithPath.foldLeft(into) {
              case (acc, (jpath, JNothing)) => acc
              case (acc, (jpath, v)) =>
                val ctype = CType.forJValue(v) getOrElse { sys.error("Cannot determine ctype for " + v + " at " + jpath + " in " + jv) }
                val ref = ColumnRef(jpath, ctype)
  
                val pair: (BitSet, Array[_]) = v match {
                  case JBool(b) => 
                    val (defined, col) = acc.getOrElse(ref, (BitSet(), new Array[Boolean](sliceSize))).asInstanceOf[(BitSet, Array[Boolean])]
                    col(sliceIndex) = b
                    (defined + sliceIndex, col)
                    
                  case JNum(d) => {
                    val isLong = ctype == CLong
                    val isDouble = ctype == CDouble
                    
                    val (defined, col) = if (isLong) {
                      val (defined, col) = acc.getOrElse(ref, (BitSet(), new Array[Long](sliceSize))).asInstanceOf[(BitSet, Array[Long])]
                      col(sliceIndex) = d.toLong
                      (defined, col)
                    } else if (isDouble) {
                      val (defined, col) = acc.getOrElse(ref, (BitSet(), new Array[Double](sliceSize))).asInstanceOf[(BitSet, Array[Double])]
                      col(sliceIndex) = d.toDouble
                      (defined, col)
                    } else {
                      val (defined, col) = acc.getOrElse(ref, (BitSet(), new Array[BigDecimal](sliceSize))).asInstanceOf[(BitSet, Array[BigDecimal])]
                      col(sliceIndex) = d
                      (defined, col)
                    }
                    
                    (defined + sliceIndex, col)
                  }
  
                  case JString(s) => 
                    val (defined, col) = acc.getOrElse(ref, (BitSet(), new Array[String](sliceSize))).asInstanceOf[(BitSet, Array[String])]
                    col(sliceIndex) = s
                    (defined + sliceIndex, col)
                  
                  case JArray(Nil)  => 
                    val (defined, col) = acc.getOrElse(ref, (BitSet(), null)).asInstanceOf[(BitSet, Array[Boolean])]
                    (defined + sliceIndex, col)
  
                  case JObject(Nil) => 
                    val (defined, col) = acc.getOrElse(ref, (BitSet(), null)).asInstanceOf[(BitSet, Array[Boolean])]
                    (defined + sliceIndex, col)
  
                  case JNull        => 
                    val (defined, col) = acc.getOrElse(ref, (BitSet(), null)).asInstanceOf[(BitSet, Array[Boolean])]
                    (defined + sliceIndex, col)
                }
  
                acc + (ref -> pair)
            }

            //println("Computed " + withIdsAndValues)
  
            buildColArrays(xs, withIdsAndValues, sliceIndex + 1)
  
          case _ => (into, sliceIndex)
        }
      }
  
      // FIXME: If prefix is empty (eg. because sampleData.data is empty) the generated
      // columns won't satisfy sampleData.schema. This will cause the subsumption test in
      // Slice#typed to fail unless it allows for vacuous success
      val slice = new Slice {
        val (cols, size) = buildColArrays(prefix.toStream, Map.empty[ColumnRef, (BitSet, Array[_])], 0) 
        val columns = cols map {
          case (ref @ ColumnRef(_, CBoolean), (defined, values))     => (ref, ArrayBoolColumn(defined, values.asInstanceOf[Array[Boolean]]))
          case (ref @ ColumnRef(_, CLong), (defined, values))        => (ref, ArrayLongColumn(defined, values.asInstanceOf[Array[Long]]))
          case (ref @ ColumnRef(_, CDouble), (defined, values))      => (ref, ArrayDoubleColumn(defined, values.asInstanceOf[Array[Double]]))
          case (ref @ ColumnRef(_, CNum), (defined, values))         => (ref, ArrayNumColumn(defined, values.asInstanceOf[Array[BigDecimal]]))
          case (ref @ ColumnRef(_, CString), (defined, values))      => (ref, ArrayStrColumn(defined, values.asInstanceOf[Array[String]]))
          case (ref @ ColumnRef(_, CEmptyArray), (defined, values))  => (ref, new BitsetColumn(defined) with EmptyArrayColumn)
          case (ref @ ColumnRef(_, CEmptyObject), (defined, values)) => (ref, new BitsetColumn(defined) with EmptyObjectColumn)
          case (ref @ ColumnRef(_, CNull), (defined, values))        => (ref, new BitsetColumn(defined) with NullColumn)
        }
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
      }
    )
  }

  def lookupF1(namespace: List[String], name: String): F1 = {
    val lib = Map[String, CF1](
      "negate" -> cf.math.Negate,
      "true" -> new CF1P({ case _ => Column.const(true) })
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
          val identityPath = cols collect { case c @ (ColumnRef(JPath.Identity, _), _) => c }
          val prioritized = identityPath.values filter {
            case (_: LongColumn | _: DoubleColumn | _: NumColumn) => true
            case _ => false
          }
          
          val mask = BitSet(range filter { i => prioritized exists { _ isDefinedAt i } }: _*)
          
          val (a2, arr) = mask.foldLeft((a, new Array[BigDecimal](range.end))) {
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
          
          (a2, Map(ColumnRef(JPath.Identity, CNum) -> ArrayNumColumn(mask, arr)))
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
