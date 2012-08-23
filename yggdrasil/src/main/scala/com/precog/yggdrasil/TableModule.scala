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

import com.precog.common.json._
import com.precog.common.Path
import com.precog.bytecode.JType
import blueeyes.json.JsonAST._

import collection.Set

import scalaz.Monoid
import scalaz.Monad

object TableModule {
  object paths {
    val Key   = CPathField("key")
    val Value = CPathField("value")
    val Group = CPathField("group")
    val SortKey = CPathField("sortkey")
  }  
}

trait TableModule[M[+_]] extends FNModule {
  type UserId
  type Scanner
  type Reducer[α]

  implicit def M: Monad[M]

  object trans {
    sealed trait TransSpec[+A <: SourceType]
    sealed trait SourceType
  
    sealed trait Source1 extends SourceType
    case object Source extends Source1
    
    sealed trait Source2 extends SourceType
    case object SourceLeft extends Source2
    case object SourceRight extends Source2
    
    case class Leaf[+A <: SourceType](source: A) extends TransSpec[A] //done
    
    case class Filter[+A <: SourceType](source: TransSpec[A], predicate: TransSpec[A]) extends TransSpec[A] //done
    
    // Adds a column to the output in the manner of scanLeft
    case class Scan[+A <: SourceType](source: TransSpec[A], scanner: Scanner) extends TransSpec[A] //done
    
    case class Map1[+A <: SourceType](source: TransSpec[A], f: F1) extends TransSpec[A] //done
    
    // apply a function to the cartesian product of the transformed left and right subsets of columns
    case class Map2[+A <: SourceType](left: TransSpec[A], right: TransSpec[A], f: F2) extends TransSpec[A] //done
    
    // Perform the specified transformation on the left and right sides, and then create a new set of columns
    // containing all the resulting columns.
    case class ObjectConcat[+A <: SourceType](left: TransSpec[A], right: TransSpec[A]) extends TransSpec[A] //done

    case class ObjectDelete[+A <: SourceType](source: TransSpec[A], fields: Set[CPathField]) extends TransSpec[A]
    
    case class ArrayConcat[+A <: SourceType](left: TransSpec[A], right: TransSpec[A]) extends TransSpec[A] //done
    
    // Take the output of the specified TransSpec and prefix all of the resulting selectors with the
    // specified field. 
    case class WrapObject[+A <: SourceType](source: TransSpec[A], field: String) extends TransSpec[A] //done
    
    case class WrapObjectDynamic[+A <: SourceType](left: TransSpec[A], right: TransSpec[A]) extends TransSpec[A]
    
    case class WrapArray[+A <: SourceType](source: TransSpec[A]) extends TransSpec[A] //done
    
    case class DerefObjectStatic[+A <: SourceType](source: TransSpec[A], field: CPathField) extends TransSpec[A] //done
    
    case class DerefMetadataStatic[+A <: SourceType](source: TransSpec[A], field: CPathMeta) extends TransSpec[A]
    
    case class DerefObjectDynamic[+A <: SourceType](left: TransSpec[A], right: TransSpec[A]) extends TransSpec[A] //done
    
    case class DerefArrayStatic[+A <: SourceType](source: TransSpec[A], element: CPathIndex) extends TransSpec[A] //done
    
    case class DerefArrayDynamic[+A <: SourceType](left: TransSpec[A], right: TransSpec[A]) extends TransSpec[A] //done
    
    case class ArraySwap[+A <: SourceType](source: TransSpec[A], index: Int) extends TransSpec[A]
    
    // Filter out all the source columns whose selector and CType is not specified by the supplied JType
    // if the set of columns does not cover the JType specified, this will return the empty slice.
    case class Typed[+A <: SourceType](source: TransSpec[A], tpe: JType) extends TransSpec[A] // done
    
    case class Equal[+A <: SourceType](left: TransSpec[A], right: TransSpec[A]) extends TransSpec[A] //done

    case class EqualLiteral[+A <: SourceType](left: TransSpec[A], right: CValue, invert: Boolean) extends TransSpec[A]
    
    case class ConstLiteral[+A <: SourceType](value: CValue, target: TransSpec[A]) extends TransSpec[A]
  
    type TransSpec1 = TransSpec[Source1]
    
    object TransSpec1 {
      val Id = Leaf(Source)
    }
    
    type TransSpec2 = TransSpec[Source2]
    
    object TransSpec2 {
      val LeftId = Leaf(SourceLeft)
      val RightId = Leaf(SourceRight)
    }
  
    sealed trait GroupKeySpec
    
    /**
     * Definition for a single (non-composite) key part.
     *
     * @param key The key which will be used by `merge` to access this particular tic-variable (which may be refined by more than one `GroupKeySpecSource`)
     * @param spec A transform which defines this key part as a function of the source table in `GroupingSource`.
     */
    case class GroupKeySpecSource(key: CPathField, spec: TransSpec1) extends GroupKeySpec
    
    case class GroupKeySpecAnd(left: GroupKeySpec, right: GroupKeySpec) extends GroupKeySpec
    case class GroupKeySpecOr(left: GroupKeySpec, right: GroupKeySpec) extends GroupKeySpec
    
    sealed trait GroupingSpec[GroupId]
    
    /**
     * Definition for a single group set and its associated composite key part.
     *
     * @param table The target set for the grouping
     * @param targetTrans The key which will be used by `merge` to access a particular subset of the target
     * @param groupKeySpec A composite union/intersect overlay on top of transspec indicating the composite key for this target set
     */
    final case class GroupingSource[GroupId: scalaz.Equal](table: Table, targetTrans: TransSpec1, groupId: GroupId, groupKeySpec: GroupKeySpec) extends GroupingSpec[GroupId]
    
    final case class GroupingUnion[GroupId: scalaz.Equal](groupKeyLeftTrans: TransSpec1, groupKeyRightTrans: TransSpec1, left: GroupingSpec[GroupId], right: GroupingSpec[GroupId], align: GroupKeyAlign) extends GroupingSpec[GroupId]
    final case class GroupingIntersect[GroupId: scalaz.Equal](groupKeyLeftTrans: TransSpec1, groupKeyRightTrans: TransSpec1, left: GroupingSpec[GroupId], right: GroupingSpec[GroupId], align: GroupKeyAlign) extends GroupingSpec[GroupId]
    
    sealed trait GroupKeyAlign
    
    object GroupKeyAlign {
      case object Eq extends GroupKeyAlign
    
      /*
      case object Neq extends GroupKeyAlign
      case object Lte extends GroupKeyAlign
      case object Lt extends GroupKeyAlign
      case object Gt extends GroupKeyAlign
      case object Gte extends GroupKeyAlign
      */
    }
    
    sealed trait SortOrder
    sealed trait DesiredSortOrder extends SortOrder {
      def isAscending: Boolean
    }
    case object SortAscending extends DesiredSortOrder { val isAscending = true }
    case object SortDescending extends DesiredSortOrder { val isAscending = false }
    case object SortUnknown extends SortOrder
    
    object constants {
      import TableModule.paths._

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

      object SourceSortKey {
        val Single = DerefObjectStatic(Leaf(Source), SortKey)
        
        val Left = DerefObjectStatic(Leaf(SourceLeft), SortKey)
        val Right = DerefObjectStatic(Leaf(SourceRight), SortKey)
      }

    }
  }
  
  trait TableOps {
    def empty: Table
    
    def constString(v: Set[CString]): Table
    def constLong(v: Set[CLong]): Table
    def constDouble(v: Set[CDouble]): Table
    def constDecimal(v: Set[CNum]): Table
    def constBoolean(v: Set[CBoolean]): Table
    def constNull: Table
    
    def constEmptyObject: Table
    def constEmptyArray: Table
  }
  
  def ops: TableOps
  def grouper: Grouper
  
  type Table <: TableLike
  
  trait Grouper {
    import trans._

    /**
     * @param grouping The group spec
     * @param body The evaluator, taking a ''map'' from a key to some table (representing a tic variable or group set)
     */
    def merge[GroupId: scalaz.Equal](grouping: GroupingSpec[GroupId])(body: (Table, GroupId => Table) => M[Table]): M[Table]
  }
    
  trait TableLike { this: Table =>
    import trans._

    /**
     * For each distinct path in the table, load all columns identified by the specified
     * jtype and concatenate the resulting slices into a new table.
     */
    def load(uid: UserId, tpe: JType): M[Table]
    
    /**
     * Folds over the table to produce a single value (stored in a singleton table).
     */
    def reduce[A: Monoid](reducer: Reducer[A]): M[A]
    
    /**
     * Removes all rows in the table for which all values are undefined. 
     * Remaps the indicies.
     */
    def compact(spec: TransSpec1): Table

    /**
     * Performs a one-pass transformation of the keys and values in the table.
     * If the key transform is not identity, the resulting table will have
     * unknown sort order.
     */
    def transform(spec: TransSpec1): Table
    
    /**
     * Cogroups this table with another table, using equality on the specified
     * transformation on rows of the table.
     */
    def cogroup(leftKey: TransSpec1, rightKey: TransSpec1, that: Table)(left: TransSpec1, right: TransSpec1, both: TransSpec2): Table
    
    /**
     * Performs a full cartesian cross on this table with the specified table,
     * applying the specified transformation to merge the two tables into
     * a single table.
     */
    def cross(that: Table)(spec: TransSpec2): Table
    
    /**
     * Sorts the KV table by ascending or descending order of a transformation
     * applied to the rows.
     */
    def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder): M[Table]
    
    def distinct(spec: TransSpec1): Table
    
    def group[GroupId: scalaz.Equal](trans: TransSpec1, groupId: GroupId, groupKeySpec: GroupKeySpec): GroupingSpec[GroupId] = GroupingSource[GroupId](this, trans, groupId, groupKeySpec)
    
    def drop(n: Long): Table
    
    def take(n: Long): Table
    
    def takeRight(n: Long): Table

    def toJson: M[Iterable[JValue]]
  }
}
