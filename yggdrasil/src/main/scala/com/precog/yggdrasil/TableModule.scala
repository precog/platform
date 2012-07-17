package com.precog.yggdrasil

import com.precog.common.Path
import com.precog.bytecode.JType
import blueeyes.json.{JPath, JPathField, JPathIndex}
import blueeyes.json.JsonAST._
import akka.dispatch.Future
import scalaz.Monoid

import collection.Set

trait TableModule extends FNModule {
  type Scanner
  type Reducer[α]

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
    
    case class ArrayConcat[+A <: SourceType](left: TransSpec[A], right: TransSpec[A]) extends TransSpec[A] //done
    
    // Take the output of the specified TransSpec and prefix all of the resulting selectors with the
    // specified field. 
    case class WrapObject[+A <: SourceType](source: TransSpec[A], field: String) extends TransSpec[A] //done
    
    case class WrapObjectDynamic[+A <: SourceType](left: TransSpec[A], right: TransSpec[A]) extends TransSpec[A]
    
    case class WrapArray[+A <: SourceType](source: TransSpec[A]) extends TransSpec[A] //done
    
    case class DerefObjectStatic[+A <: SourceType](source: TransSpec[A], field: JPathField) extends TransSpec[A] //done
    
    case class DerefObjectDynamic[+A <: SourceType](left: TransSpec[A], right: TransSpec[A]) extends TransSpec[A] //done
    
    case class DerefArrayStatic[+A <: SourceType](source: TransSpec[A], element: JPathIndex) extends TransSpec[A] //done
    
    case class DerefArrayDynamic[+A <: SourceType](left: TransSpec[A], right: TransSpec[A]) extends TransSpec[A] //done
    
    case class ArraySwap[+A <: SourceType](source: TransSpec[A], index: Int) extends TransSpec[A]
    
    // Filter out all the source columns whose selector and CType is not specified by the supplied JType
    // if the set of columns does not cover the JType specified, this will return the empty slice.
    case class Typed[+A <: SourceType](source: TransSpec[A], tpe: JType) extends TransSpec[A] // done
    
    case class Equal[+A <: SourceType](left: TransSpec[A], right: TransSpec[A]) extends TransSpec[A] //done
  
    type TransSpec1 = TransSpec[Source1]
    
    object TransSpec1 {
      val Id = Leaf(Source)
    }
    
    type TransSpec2 = TransSpec[Source2]
    
    object TransSpec2 {
      val LeftId = Leaf(SourceLeft)
      val RightId = Leaf(SourceRight)
    }
  
    sealed trait GroupKeySpec[A]
    
    /**
     * Definition for a single (non-composite) key part.
     *
     * @param a The key which will be used by `merge` to access this particular tic-variable (which may be refined by more than one `GroupKeySpecSource`)
     * @param spec A transform which defines this key part as a function of the source table in `GroupingSource`.
     */
    case class GroupKeySpecSource[A: scalaz.Equal](a: A, spec: TransSpec1) extends GroupKeySpec[A]
    
    case class GroupKeySpecAnd[A: scalaz.Equal](left: GroupKeySpec[A], right: GroupKeySpec[A]) extends GroupKeySpec[A]
    case class GroupKeySpecOr[A: scalaz.Equal](left: GroupKeySpec[A], right: GroupKeySpec[A]) extends GroupKeySpec[A]
    
    sealed trait GroupingSpec[A]
    
    /**
     * Definition for a single group set and its associated composite key part.
     *
     * @param table The target set for the grouping
     * @param a The key which will be used by `merge` to access a particular subset of the target
     * @param groupKeySpec A composite union/intersect overlay on top of transspec indicating the composite key for this target set
     */
    final case class GroupingSource[A: scalaz.Equal](table: Table, a: A, groupKeySpec: GroupKeySpec[A]) extends GroupingSpec[A]
    
    final case class GroupingUnion[A: scalaz.Equal](groupKeyLeftTrans: TransSpec1, groupKeyRightTrans: TransSpec1, left: GroupingSpec[A], right: GroupingSpec[A], align: GroupKeyAlign) extends GroupingSpec[A]
    final case class GroupingIntersect[A: scalaz.Equal](groupKeyLeftTrans: TransSpec1, groupKeyRightTrans: TransSpec1, left: GroupingSpec[A], right: GroupingSpec[A], align: GroupKeyAlign) extends GroupingSpec[A]
    
    trait Grouper {
      /**
       * @param grouping The group spec
       * @param body The evaluator, taking a ''map'' from a key to some table (representing a tic variable or group set)
       */
      def merge[A: scalaz.Equal](grouping: GroupingSpec[A])(body: (A => Table) => Table): Table
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
    
    object constants {
      val Key   = JPathField("key")
      val Value = JPathField("value")
      val Group = JPathField("group")
      
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
  
  type Table <: TableLike
  
  trait TableLike { this: Table =>
    import trans._

    /**
     * For each distinct path in the table, load all columns identified by the specified
     * jtype and concatenate the resulting slices into a new table.
     */
    def load(tpe: JType): Future[Table]
    
    /**
     * Folds over the table to produce a single value (stored in a singleton table).
     */
    def reduce[A: Monoid](reducer: Reducer[A]): A
    
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
    def sort(sortKey: TransSpec1, sortOrder: SortOrder): Table
    
    def group[A: scalaz.Equal](a: A, groupKeySpec: GroupKeySpec[A]): GroupingSpec[A] = GroupingSource[A](this, a, groupKeySpec)
    
    def drop(n: Long): Table
    
    def take(n: Long): Table
    
    def takeRight(n: Long): Table

    def toJson: Iterable[JValue]
  }
}
