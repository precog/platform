package com.precog.yggdrasil

import blueeyes.json.JPath

trait TableModule extends Schema {
  type F1
  type F2     // needs pimped: partialRight(CValue): F1 and partialLeft(CValue): F1

  // TODO something saner than a structural type here
  implicit def pimpF2(f2: F2): PartiallyApplied

  trait PartiallyApplied {
    def partialLeft(cv: CValue): F1 
    def partialRight(cv: CValue): F1
  }
  
  object trans {
    sealed trait TransSpec[+A <: SourceType]
    sealed trait SourceType
  
    sealed trait Source1 extends SourceType
    case object Source extends Source1
    
    sealed trait Source2 extends SourceType
    case object SourceLeft extends Source2
    case object SourceRight extends Source2
    
    case class Leaf[+A <: SourceType](source: A) extends TransSpec[A]
    
    case class Filter[+A <: SourceType](target: TransSpec[A], predicate: TransSpec[A]) extends TransSpec[A]
    
    case class Scan[+A <: SourceType, B](target: TransSpec[A], scanner: Scanner[_, _, _]) extends TransSpec[A]
    
    case class Map1[+A <: SourceType](target: TransSpec[A], f: F1) extends TransSpec[A]
    
    case class Map2[+A <: SourceType](left: TransSpec[A], right: TransSpec[A], f: F2) extends TransSpec[A]
    
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
    
    object TransSpec1 {
      val Id = Leaf(Source)
    }
    
    type TransSpec2 = TransSpec[Source2]
    
    object TransSpec2 {
      val LeftId = Leaf(SourceLeft)
      val RightId = Leaf(SourceRight)
    }
    
    type TableTransSpec[+A <: SourceType] = Map[JPath, TransSpec[A]]
    type TableTransSpec1 = TableTransSpec[Source1]
    type TableTransSpec2 = TableTransSpec[Source2]
    
    object TableTransSpec {
      def makeTransSpec[A <: SourceType](tableTrans: TableTransSpec[A]): TransSpec[A] =
        sys.error("TODO")
    }
  
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
    
    object constants {
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
    
    def constString(v: String): Table
    def constLong(v: Long): Table
    def constDouble(v: Double): Table
    def constDecimal(v: BigDecimal): Table
    def constBoolean(v: Boolean): Table
    def constNull: Table
    
    def constEmptyObject: Table
    def constEmptyArray: Table
  }
  
  def ops: TableOps
  
  type Table <: TableLike
  
  trait TableLike { this: Table =>
    import trans._
    
    /**
     * Folds over the table to produce a single value (stored in a singleton table).
     */
    def reduce(scanner: Scanner[_, _, _]): Table
    
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
    
    def group[A: scalaz.Equal](a: A, groupKeySpec: GroupKeySpec): GroupingSpec[A] = GroupingSource[A](this, a, groupKeySpec)
    
    // Does this have to be fully known at every point in time?
    def schema: JType
    
    def drop(n: Int): Table
    
    def take(n: Int): Table
    
    def takeRight(n: Int): Table
  }
}
