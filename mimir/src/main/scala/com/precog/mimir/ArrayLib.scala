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
package com.precog
package mimir

import com.precog.bytecode._
import com.precog.common._

import com.precog.yggdrasil._
import com.precog.yggdrasil.table._

trait ArrayLibModule[M[+_]] extends ColumnarTableLibModule[M] {
  trait ArrayLib extends ColumnarTableLib {
    override def _libMorphism1 = super._libMorphism1 ++ Set(Flatten)
    
    object Flatten extends Morphism1(Vector(), "flatten") {
      import trans._
      import TransSpecModule._
      import scalaz.syntax.monad._
      
      val tpe = UnaryOperationType(JArrayUnfixedT, JType.JUniverseT)
      
      override val idPolicy = IdentityPolicy.Product(IdentityPolicy.Retain.Merge, IdentityPolicy.Synthesize)
      
      def apply(table: Table, ctx: MorphContext) = M point {
        val derefed = table transform trans.DerefObjectStatic(Leaf(Source), paths.Value)
        val keys = table transform trans.DerefObjectStatic(Leaf(Source), paths.Key)
        
        val flattenedSlices = table.slices map { slice =>
          val keys = slice.deref(paths.Key)
          val values = slice.deref(paths.Value)
          
          val indices = values.columns.keys collect {
            case ColumnRef(CPath(CPathIndex(i), _ @ _*), _) => i
          }
          
          if (indices.isEmpty) {
            Slice.empty
          } else {
            val maxLength = indices.max + 1
  
            val columnTables = values.columns.foldLeft(Map[ColumnRef, Array[Column]]()) {
              case (acc, (ColumnRef(CPath(CPathIndex(idx), ptail @ _*), tpe), col)) => {
                // remap around the mod ring w.r.t. max length
                // s.t. f(i) = f'(i * max + arrayI)
                
                val finalRef = ColumnRef(CPath(ptail: _*), tpe)
                val colTable = acc get finalRef getOrElse (new Array[Column](maxLength))
                
                colTable(idx) = col
                
                acc.updated(finalRef, colTable)
              }
              
              case (acc, _) => acc
            }
            
            val valueCols = columnTables map {
              case (ref @ ColumnRef(_, CUndefined), _) =>
                ref -> UndefinedColumn.raw
              
              case (ref @ ColumnRef(_, CBoolean), colTable) => {
                val col = new ModUnionColumn(colTable) with BoolColumn {
                  def apply(i: Int) = col(i).asInstanceOf[BoolColumn](row(i))
                }
                
                ref -> col
              }
              
              case (ref @ ColumnRef(_, CString), colTable) => {
                val col = new ModUnionColumn(colTable) with StrColumn {
                  def apply(i: Int) = col(i).asInstanceOf[StrColumn](row(i))
                }
                
                ref -> col
              }
              
              case (ref @ ColumnRef(_, CLong), colTable) => {
                val col = new ModUnionColumn(colTable) with LongColumn {
                  def apply(i: Int) = col(i).asInstanceOf[LongColumn](row(i))
                }
                
                ref -> col
              }
              
              case (ref @ ColumnRef(_, CDouble), colTable) => {
                val col = new ModUnionColumn(colTable) with DoubleColumn {
                  def apply(i: Int) = col(i).asInstanceOf[DoubleColumn](row(i))
                }
                
                ref -> col
              }
              
              case (ref @ ColumnRef(_, CNum), colTable) => {
                val col = new ModUnionColumn(colTable) with NumColumn {
                  def apply(i: Int) = col(i).asInstanceOf[NumColumn](row(i))
                }
                
                ref -> col
              }
              
              case (ref @ ColumnRef(_, CEmptyObject), colTable) => {
                val col = new ModUnionColumn(colTable) with EmptyObjectColumn
                
                ref -> col
              }
              
              case (ref @ ColumnRef(_, CEmptyArray), colTable) => {
                val col = new ModUnionColumn(colTable) with EmptyArrayColumn
                
                ref -> col
              }
              
              case (ref @ ColumnRef(_, CNull), colTable) => {
                val col = new ModUnionColumn(colTable) with NullColumn
                
                ref -> col
              }
              
              case (ref @ ColumnRef(_, CDate), colTable) => {
                val col = new ModUnionColumn(colTable) with DateColumn {
                  def apply(i: Int) = col(i).asInstanceOf[DateColumn](row(i))
                }
                
                ref -> col
              }
  
              case (ref @ ColumnRef(_, CPeriod), colTable) => {
                val col = new ModUnionColumn(colTable) with PeriodColumn {
                  def apply(i: Int) = col(i).asInstanceOf[PeriodColumn](row(i))
                }
                
                ref -> col
              }
              
              case (ref @ ColumnRef(_, arrTpe: CArrayType[a]), colTable) => {
                val col = new ModUnionColumn(colTable) with HomogeneousArrayColumn[a] {
                  val tpe = arrTpe
                  def apply(i: Int) = col(i).asInstanceOf[HomogeneousArrayColumn[a]](row(i))      // primitive arrays are still objects, so the erasure here is not a problem
                }
                
                ref -> col
              }
            }
  
            val remap = cf.util.Remap(_ / maxLength)
            val keyCols = for {
              (ref, col) <- keys.columns
              col0 <- remap(col)
            } yield (ref -> col0)
  
            val sliceSize = maxLength * slice.size
            val keySlice = Slice(keyCols, sliceSize).wrap(paths.Key)
            val valueSlice = Slice(valueCols, sliceSize).wrap(paths.Value)
            keySlice zip valueSlice
          }
        }
        
        val size2 = UnknownSize
        val flattenedTable = Table(flattenedSlices, UnknownSize).compact(TransSpec1.Id)
        val finalTable = flattenedTable.canonicalize(yggConfig.minIdealSliceSize, Some(yggConfig.maxSliceSize))
        
        val spec = InnerObjectConcat(
          WrapObject(
            InnerArrayConcat(
              DerefObjectStatic(Leaf(Source), paths.Key),
              WrapArray(Scan(Leaf(Source), freshIdScanner))
            ),
            paths.Key.name),
          WrapObject(
            DerefObjectStatic(Leaf(Source), paths.Value),
            paths.Value.name))
        
        finalTable transform spec
      }
    }
  }
}
