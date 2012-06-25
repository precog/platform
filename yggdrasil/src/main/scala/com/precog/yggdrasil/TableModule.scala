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

import blueeyes.json.JPath

trait TableModule extends Schema {
  type F1
  type F2

  def liftF1(f: CValue => CValue): F1
  
  object transforms {
    sealed trait TransSpec[+A <: SourceType]
    sealed trait SourceType
  
    sealed trait Source1 extends SourceType
    case object Source extends Source1
    
    sealed trait Source2 extends SourceType
    case object SourceLeft extends Source2
    case object SourceRight extends Source2
    
    // why do we need an explicit leaf?
    case class Leaf[+A <: SourceType](source: A) extends TransSpec[A]
    
    // why does filter take a left and a right?
    case class Filter[+A <: SourceType](target: TransSpec[A], predicate: TransSpec[A]) extends TransSpec[A]
    
    case class Scan[+A <: SourceType, B](target: TransSpec[A], scanner: Scanner[_, _, _]) extends TransSpec[A]
    
    case class Map1[+A <: SourceType](target: TransSpec[A])(f: F1) extends TransSpec[A]
    
    case class Map2[+A <: SourceType](left: TransSpec[A], right: TransSpec[A])(f: F2) extends TransSpec[A]
    
    case class ObjectConcat[+A <: SourceType](left: TransSpec[A], right: TransSpec[A]) extends TransSpec[A]
    
    case class WrapStatic[+A <: SourceType](target: TransSpec[A], field: String) extends TransSpec[A]
    
    case class WrapDynamic[+A <: SourceType](left: TransSpec[A], right: TransSpec[A]) extends TransSpec[A]
    
    case class ArrayConcat[+A <: SourceType](left: TransSpec[A], right: TransSpec[A]) extends TransSpec[A]
    
    case class ArraySwap[+A <: SourceType](target: TransSpec[A], index: Long) extends TransSpec[A]
    
    case class DerefObjectStatic[+A <: SourceType](target: TransSpec[A], field: JPath) extends TransSpec[A]
    
    case class DerefObjectDynamic[+A <: SourceType](left: TransSpec[A], right: TransSpec[A]) extends TransSpec[A]
    
    case class DerefArrayStatic[+A <: SourceType](target: TransSpec[A], element: Int) extends TransSpec[A]
    
    case class DerefArrayDynamic[+A <: SourceType](left: TransSpec[A], right: TransSpec[A]) extends TransSpec[A]
    
    case class Typed[+A <: SourceType](target: TransSpec[A], tpe: JType) extends TransSpec[A]
    
    case class Equal[+A <: SourceType](left: TransSpec[A], right: TransSpec[A]) extends TransSpec[A]
  
    type TransSpec1 = TransSpec[Source1]
    type TransSpec2 = TransSpec[Source2]
    type TableTransSpec[+A <: SourceType] = Map[JPath, TransSpec[A]]
    type TableTransSpec1 = TableTransSpec[Source1]
    type TableTransSpec2 = TableTransSpec[Source2]
  
    sealed trait GroupKeySpec
    case class GroupKeySpecSource(selector: JPath, spec: TransSpec1) extends GroupKeySpec
    case class GroupKeySpecAnd(left: GroupKeySpec, right: GroupKeySpec) extends GroupKeySpec
    case class GroupKeySpecOr(left: GroupKeySpec, right: GroupKeySpec) extends GroupKeySpec
    
    sealed trait GroupingSpec[A]
    final case class GroupingSource[A: scalaz.Equal](table: Table, a: A, groupKeySpec: GroupKeySpec) extends GroupingSpec[A]
    final case class GroupingUnion[A: scalaz.Equal](groupKeyLeftTrans: TransSpec1, groupKeyRightTrans: TransSpec1, left: GroupingSpec[A], right: GroupingSpec[A], align: GroupKeyAlign) extends GroupingSpec[A]
    final case class GroupingIntersect[A: scalaz.Equal](groupKeyLeftTrans: TransSpec1, groupKeyRightTrans: TransSpec1, left: GroupingSpec[A], right: GroupingSpec[A], align: GroupKeyAlign) extends GroupingSpec[A]
    
    trait Grouper {
      def merge[A: scalaz.Equal](grouping: GroupingSpec[A])(body: (Table, A => Table) => Table): Table
    }
    
    sealed trait GroupKeyAlign
    case object EQ extends GroupKeyAlign
    
    /*
    case object NEQ extends GroupKeyAlign
    case object LTE extends GroupKeyAlign
    case object LT extends GroupKeyAlign
    case object GT extends GroupKeyAlign
    case object GTE extends GroupKeyAlign
    */
    
    sealed trait SortOrder
    case object SortAscending extends SortOrder
    case object SortDescending extends SortOrder
    case object SortUnknown extends SortOrder
    
    object TableKVConstants {
      val Key   = JPath("key")
      val Value = JPath("value")
      val Group = JPath("group")
      
      object SourceKey {
        val Single = DerefObjectStatic(Leaf(Source), Key)
        
        val Left = DerefObjectStatic(Leaf(SourceLeft), Key)
        val Right = DerefObjectStatic(Leaf(SourceRight), Key)
      }
      
      object SourceValue {
        val Single = DerefObjectStatic(Leaf(Source), Value)
        
        val Left = DerefObjectStatic(Leaf(SourceLeft), Value)
        val Right = DerefObjectStatic(Leaf(SourceRight), Value)
      }
      
      object SourceGroup {
        val Single = DerefObjectStatic(Leaf(Source), Group)
        
        val Left = DerefObjectStatic(Leaf(SourceLeft), Group)
        val Right = DerefObjectStatic(Leaf(SourceRight), Group)
      }
    }
  }
  
  trait TableOps {
    def loadStatic(path: String): Table
    def loadDynamic(source: Table): Table
    
    def empty: Table
    
    def const(v: String): Table
    def const(v: Long): Table
    def const(v: Double): Table
    def const(v: BigDecimal): Table
    def const(v: Boolean): Table
    def constNull: Table
  }
  
  def ops: TableOps
  
  type Table <: TableLike
  
  trait TableLike { this: Table =>
    import transforms._
    
    /**
     * Folds over the table to produce a single value (stored in a singleton table).
     */
    def reduce(scanner: Scanner[_, _, _]): Table
    
    /**
     * Performs a one-pass transformation of the keys and values in the table.
     * If the key transform is not identity, the resulting table will have
     * unknown sort order.
     */
    def transform(spec: TableTransSpec1): Table
    
    /**
     * Cogroups this table with another table, using equality on the specified
     * transformation on rows of the table.
     */
    def cogroup(leftKey: TransSpec1, rightKey: TransSpec1, that: Table)(left: TableTransSpec1, right: TableTransSpec1, both: TableTransSpec2): Table
    
    /**
     * Performs a full cartesian cross on this table with the specified table,
     * applying the specified transformation to merge the two tables into
     * a single table.
     */
    def cross(that: Table)(spec: TableTransSpec2): Table
    
    /**
     * Sorts the KV table by ascending or descending order of a transformation
     * applied to the rows.
     */
    def sort(sortKey: TransSpec1, sortOrder: SortOrder): Table
    
    def group[A: scalaz.Equal](a: A, groupKeySpec: GroupKeySpec): GroupingSpec[A] = GroupingSource[A](this, a, groupKeySpec)
    
    // Does this have to be fully known at every point in time?
    def schema: JType
    
    def drop(n: Int): Table
    
    def take(n: Int): Table
    
    def takeRight(n: Int): Table
  }
}
