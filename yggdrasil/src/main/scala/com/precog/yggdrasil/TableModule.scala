package com.precog.yggdrasil

import com.precog.common.Path
import com.precog.bytecode.JType
import blueeyes.json.{JPath, JPathField, JPathIndex}
import blueeyes.json.JsonAST._

import collection.Set

import scalaz.Monoid
import scalaz.Monad

object TableModule {
  object paths {
    val Key   = JPathField("key")
    val Value = JPathField("value")
    val Group = JPathField("group")
    val SortKey = JPathField("sortkey")
  }  
}

trait TableModule[M[+_]] extends FNModule {
  type UserId
  type GroupId
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

    case class ObjectDelete[+A <: SourceType](source: TransSpec[A], fields: Set[JPathField]) extends TransSpec[A]
    
    // TODO: Make ArrayConcat n-ary
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

    case class EqualLiteral[+A <: SourceType](left: TransSpec[A], right: CValue, invert: Boolean) extends TransSpec[A]
    
    // target is the transspec that provides defineedness information. The resulting table will be defined
    // and have the constant value wherever a row provided by the target transspec has at least one member
    // that is not undefined
    case class ConstLiteral[+A <: SourceType](value: CValue, target: TransSpec[A]) extends TransSpec[A]
  
    type TransSpec1 = TransSpec[Source.type]

    object TransSpec {
      def mapSources[A <: SourceType, B <: SourceType](spec: TransSpec[A])(f: A => B): TransSpec[B] = {
        spec match {
          case Leaf(source) => Leaf(f(source))
          case trans.Filter(source, pred) => trans.Filter(mapSources(source)(f), mapSources(pred)(f))
          case Scan(source, scanner) => Scan(mapSources(source)(f), scanner)
          case trans.Map1(source, f1) => trans.Map1(mapSources(source)(f), f1)
          case trans.Map2(left, right, f2) => trans.Map2(mapSources(left)(f), mapSources(right)(f), f2)
          case trans.ObjectConcat(left, right) => trans.ObjectConcat(mapSources(left)(f), mapSources(right)(f))
          case trans.ArrayConcat(left, right) => trans.ArrayConcat(mapSources(left)(f), mapSources(right)(f))
          case trans.WrapObject(source, field) => trans.WrapObject(mapSources(source)(f), field)
          case trans.WrapArray(source) => trans.WrapArray(mapSources(source)(f))
          case DerefObjectStatic(source, field) => DerefObjectStatic(mapSources(source)(f), field)
          case DerefObjectDynamic(left, right) => DerefObjectDynamic(mapSources(left)(f), mapSources(right)(f))
          case DerefArrayStatic(source, element) => DerefArrayStatic(mapSources(source)(f), element)
          case DerefArrayDynamic(left, right) => DerefArrayDynamic(mapSources(left)(f), mapSources(right)(f))
          case trans.ArraySwap(source, index) => trans.ArraySwap(mapSources(source)(f), index)
          case Typed(source, tpe) => Typed(mapSources(source)(f), tpe)
          case trans.Equal(left, right) => trans.Equal(mapSources(left)(f), mapSources(right)(f))
          case trans.EqualLiteral(source, value, invert) => trans.EqualLiteral(mapSources(source)(f), value, invert)
        }
      }

      def deepMap[A <: SourceType](spec: TransSpec[A])(f: PartialFunction[TransSpec[A], TransSpec[A]]): TransSpec[A] = spec match {
        case x if f isDefinedAt x => f(x)
        case x @ Leaf(source) => x
        case trans.Filter(source, pred) => trans.Filter(deepMap(source)(f), deepMap(pred)(f))
        case Scan(source, scanner) => Scan(deepMap(source)(f), scanner)
        case trans.Map1(source, f1) => trans.Map1(deepMap(source)(f), f1)
        case trans.Map2(left, right, f2) => trans.Map2(deepMap(left)(f), deepMap(right)(f), f2)
        case trans.ObjectConcat(left, right) => trans.ObjectConcat(deepMap(left)(f), deepMap(right)(f))
        case trans.ArrayConcat(left, right) => trans.ArrayConcat(deepMap(left)(f), deepMap(right)(f))
        case trans.WrapObject(source, field) => trans.WrapObject(deepMap(source)(f), field)
        case trans.WrapArray(source) => trans.WrapArray(deepMap(source)(f))
        case DerefObjectStatic(source, field) => DerefObjectStatic(deepMap(source)(f), field)
        case DerefObjectDynamic(left, right) => DerefObjectDynamic(deepMap(left)(f), deepMap(right)(f))
        case DerefArrayStatic(source, element) => DerefArrayStatic(deepMap(source)(f), element)
        case DerefArrayDynamic(left, right) => DerefArrayDynamic(deepMap(left)(f), deepMap(right)(f))
        case trans.ArraySwap(source, index) => trans.ArraySwap(deepMap(source)(f), index)
        case Typed(source, tpe) => Typed(deepMap(source)(f), tpe)
        case trans.Equal(left, right) => trans.Equal(deepMap(left)(f), deepMap(right)(f))
        case trans.EqualLiteral(source, value, invert) => trans.EqualLiteral(deepMap(source)(f), value, invert)
      }
    }
    
    object TransSpec1 {
      val Id = Leaf(Source)

      val DerefArray0 = DerefArrayStatic(Leaf(Source), JPathIndex(0))
      val DerefArray1 = DerefArrayStatic(Leaf(Source), JPathIndex(1))
      val DerefArray2 = DerefArrayStatic(Leaf(Source), JPathIndex(2))
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
    case class GroupKeySpecSource(key: JPathField, spec: TransSpec1) extends GroupKeySpec
    
    case class GroupKeySpecAnd(left: GroupKeySpec, right: GroupKeySpec) extends GroupKeySpec
    case class GroupKeySpecOr(left: GroupKeySpec, right: GroupKeySpec) extends GroupKeySpec

    object GroupKeySpec {
      def dnf(keySpec: GroupKeySpec): GroupKeySpec = {
        keySpec match {
          case GroupKeySpecSource(key, spec) => GroupKeySpecSource(key, spec)
          case GroupKeySpecAnd(GroupKeySpecOr(ol, or), right) => GroupKeySpecOr(dnf(GroupKeySpecAnd(ol, right)), dnf(GroupKeySpecAnd(or, right)))
          case GroupKeySpecAnd(left, GroupKeySpecOr(ol, or)) => GroupKeySpecOr(dnf(GroupKeySpecAnd(left, ol)), dnf(GroupKeySpecAnd(left, or)))

          case gand @ GroupKeySpecAnd(left, right) => 
            val leftdnf = dnf(left)
            val rightdnf = dnf(right)
            if (leftdnf == left && rightdnf == right) gand else dnf(GroupKeySpecAnd(leftdnf, rightdnf))

          case gor @ GroupKeySpecOr(left, right) => 
            val leftdnf = dnf(left)
            val rightdnf = dnf(right)
            if (leftdnf == left && rightdnf == right) gor else dnf(GroupKeySpecOr(leftdnf, rightdnf))
        }
      }
    
      def toVector(keySpec: GroupKeySpec): Vector[GroupKeySpec] = {
        keySpec match {
          case GroupKeySpecOr(left, right) => toVector(left) ++ toVector(right)
          case x => Vector(x)
        }
      }
    }
    
    sealed trait GroupingSpec {
      def sources: Vector[GroupingSource] 
    }
    
    /**
     * Definition for a single group set and its associated composite key part.
     *
     * @param table The target set for the grouping
     * @param targetTrans The key which will be used by `merge` to access a particular subset of the target
     * @param groupKeySpec A composite union/intersect overlay on top of transspec indicating the composite key for this target set
     */
    final case class GroupingSource(table: Table, idTrans: TransSpec1, targetTrans: TransSpec1, groupId: GroupId, groupKeySpec: GroupKeySpec) extends GroupingSpec {
      def sources: Vector[GroupingSource] = Vector(this)
    }
    
    final case class GroupingAlignment(groupKeyLeftTrans: TransSpec1, groupKeyRightTrans: TransSpec1, left: GroupingSpec, right: GroupingSpec) extends GroupingSpec {
      def sources: Vector[GroupingSource] = left.sources ++ right.sources
    }

    
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
  
  trait TableCompanionLike {
    def empty: Table
    
    def constString(v: Set[CString]): Table
    def constLong(v: Set[CLong]): Table
    def constDouble(v: Set[CDouble]): Table
    def constDecimal(v: Set[CNum]): Table
    def constBoolean(v: Set[CBoolean]): Table
    def constNull: Table
    
    def constEmptyObject: Table
    def constEmptyArray: Table

    def align(sources: (Table, trans.TransSpec1)*): M[Seq[Table]]
    def intersect(sources: (Table, trans.TransSpec1)*): M[Table]
  }
  
  def ops: TableCompanion
  def grouper: Grouper
  
  type Table <: TableLike
  type TableCompanion <: TableCompanionLike
  type Grouper <: GrouperLike

  trait GrouperLike {
    import trans._

    implicit val geq: scalaz.Equal[GroupId]

    /**
     * @param grouping The group spec
     * @param body The evaluator, taking a ''map'' from a key to some table (representing a tic variable or group set)
     */
    def merge(grouping: GroupingSpec)(body: (Table, GroupId => Table) => M[Table]): M[Table]
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
    def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder = SortAscending): M[Table]
    
    def distinct(spec: TransSpec1): Table

    def groupByN(groupKeys: Seq[TransSpec1], valueSpec: TransSpec1, sortOrder: DesiredSortOrder = SortAscending): M[Seq[Table]] = sys.error("override me")
    
    def drop(n: Long): Table
    
    def take(n: Long): Table
    
    def takeRight(n: Long): Table

    def toJson: M[Iterable[JValue]]
  }
}
