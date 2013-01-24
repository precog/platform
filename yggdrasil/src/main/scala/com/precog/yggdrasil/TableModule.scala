package com.precog.yggdrasil

import com.precog.bytecode.JType
import com.precog.common.Path
import com.precog.common.json._
import com.precog.common.security._

import blueeyes.json._

import collection.Set

import scalaz.{Monad, Monoid, StreamT}

import java.nio.CharBuffer

// TODO: define better upper/lower bound methods, better comparisons,
// better names, better everything!

sealed trait TableSize {
  def maxSize: Long
  def lessThan (other: TableSize): Boolean = maxSize < other.maxSize
  def + (other: TableSize): TableSize
}

object TableSize {
  def apply(size: Long): TableSize = ExactSize(size)
  def apply(minSize: Long, maxSize: Long): TableSize =
    if (minSize != maxSize) EstimateSize(minSize, maxSize) else ExactSize(minSize)
}

case class ExactSize(minSize: Long) extends TableSize {
  val maxSize = minSize
  def + (other: TableSize) = other match {
    case ExactSize(n) => ExactSize(minSize + n)
    case EstimateSize(n1, n2) => EstimateSize(minSize + n1, minSize + n2)
    case UnknownSize => UnknownSize
  }
}

case class EstimateSize(minSize: Long, maxSize: Long) extends TableSize {
  def + (other: TableSize) = other match {
    case ExactSize(n) => EstimateSize(minSize + n, maxSize + n)
    case EstimateSize(n1, n2) => EstimateSize(minSize + n1, maxSize + n2)
    case UnknownSize => UnknownSize
  }
}

case object UnknownSize extends TableSize {
  val maxSize = Long.MaxValue
  def + (other: TableSize) = UnknownSize
}

object TableModule {
  val paths = TransSpecModule.paths
  
  sealed trait SortOrder
  sealed trait DesiredSortOrder extends SortOrder {
    def isAscending: Boolean
  }

  case object SortAscending extends DesiredSortOrder { val isAscending = true }
  case object SortDescending extends DesiredSortOrder { val isAscending = false }
  case object SortUnknown extends SortOrder
}

trait TableModule[M[+_]] extends TransSpecModule {
  import TableModule._

  type Reducer[α]
  type TableMetrics

  implicit def M: Monad[M]

  type Table <: TableLike
  type TableCompanion <: TableCompanionLike

  val Table: TableCompanion
  
  trait TableCompanionLike {
    import trans._

    def empty: Table

    def constString(v: Set[CString]): Table
    def constLong(v: Set[CLong]): Table
    def constDouble(v: Set[CDouble]): Table
    def constDecimal(v: Set[CNum]): Table
    def constDate(v: Set[CDate]): Table
    def constArray[A: CValueType](v: Set[CArray[A]]): Table
    def constBoolean(v: Set[CBoolean]): Table
    def constNull: Table
    
    def constEmptyObject: Table
    def constEmptyArray: Table

    def merge(grouping: GroupingSpec)(body: (Table, GroupId => M[Table]) => M[Table]): M[Table]
    def align(sourceLeft: Table, alignOnL: TransSpec1, sourceRight: Table, alignOnR: TransSpec1): M[(Table, Table)]
    def intersect(identitySpec: TransSpec1, tables: Table*): M[Table] 
  }

  trait TableLike { this: Table =>
    import trans._

    /**
     * Return an indication of table size, if known
     */
    def size: TableSize

    /**
     * For each distinct path in the table, load all columns identified by the specified
     * jtype and concatenate the resulting slices into a new table.
     */
    def load(apiKey: APIKey, tpe: JType): M[Table]
    
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
     * Force the table to a backing store, and provice a restartable table
     * over the results.
     */
    def force: M[Table]
    
    def paged(limit: Int): Table
    
    /**
     * Sorts the KV table by ascending or descending order of a transformation
     * applied to the rows.
     * 
     * @param sortKey The transspec to use to obtain the values to sort on
     * @param sortOrder Whether to sort ascending or descending
     * @param unique If true, the same key values will sort into a single row, otherwise
     * we assign a unique row ID as part of the key so that multiple equal values are
     * preserved
     */
    def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder = SortAscending, unique: Boolean = false): M[Table]
    
    def distinct(spec: TransSpec1): Table

    def concat(t2: Table): Table

    def toArray[A](implicit tpe: CValueType[A]): Table

    /**
     * Sorts the KV table by ascending or descending order based on a seq of transformations
     * applied to the rows.
     * 
     * @param groupKeys The transspecs to use to obtain the values to sort on
     * @param valueSpec The transspec to use to obtain the non-sorting values
     * @param sortOrder Whether to sort ascending or descending
     * @param unique If true, the same key values will sort into a single row, otherwise
     * we assign a unique row ID as part of the key so that multiple equal values are
     * preserved
     */
    def groupByN(groupKeys: Seq[TransSpec1], valueSpec: TransSpec1, sortOrder: DesiredSortOrder = SortAscending, unique: Boolean = false): M[Seq[Table]]

    def partitionMerge(partitionBy: TransSpec1)(f: Table => M[Table]): M[Table]
    
    def takeRange(startIndex: Long, numberToTake: Long): Table

    def canonicalize(length: Int, maxLength0: Option[Int] = None): Table

    def schemas: M[Set[JType]]

    def renderJson(delimiter: Char = '\n'): StreamT[M, CharBuffer]

    def renderCsv(): M[String]
    
    // for debugging only!!
    def toJson: M[Iterable[JValue]]
    
    def metrics: TableMetrics
  }

  sealed trait GroupingSpec {
    def sources: Vector[GroupingSource] 
  }

  object GroupingSpec {
    sealed trait Alignment
    case object Union extends Alignment
    case object Intersection extends Alignment
  }

  /**
   * Definition for a single group set and its associated composite key part.
   *
   * @param table The target set for the grouping
   * @param targetTrans The key which will be used by `merge` to access a particular subset of the target
   * @param groupKeySpec A composite union/intersect overlay on top of transspec indicating the composite key for this target set
   */
  final case class GroupingSource(table: Table, idTrans: trans.TransSpec1, targetTrans: Option[trans.TransSpec1], groupId: GroupId, groupKeySpec: trans.GroupKeySpec) extends GroupingSpec {
    def sources: Vector[GroupingSource] = Vector(this)
  }
  
  final case class GroupingAlignment(groupKeyLeftTrans: trans.TransSpec1, groupKeyRightTrans: trans.TransSpec1, left: GroupingSpec, right: GroupingSpec, alignment: GroupingSpec.Alignment) extends GroupingSpec {
    def sources: Vector[GroupingSource] = left.sources ++ right.sources
  }
}
