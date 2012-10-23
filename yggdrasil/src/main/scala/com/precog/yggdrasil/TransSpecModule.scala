package com.precog.yggdrasil

import com.precog.common.json._
import com.precog.common.Path
import com.precog.bytecode.JType

import blueeyes.json.JsonAST._

import collection.Set

import scalaz.{Monad, Monoid, StreamT}

import java.nio.CharBuffer

object TransSpecModule {
  object paths {
    val Key   = CPathField("key")
    val Value = CPathField("value")
    val Group = CPathField("group")
    val SortKey = CPathField("sortkey")
    val SortGlobalId = CPathField("globalid")
  }  

  sealed trait Definedness
  case object AnyDefined extends Definedness
  case object AllDefined extends Definedness
}

trait TransSpecModule extends FNModule {
  import TransSpecModule._

  type GroupId
  type Scanner

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
    
    // Perform the specified transformation on the all sources, and then create a new set of columns
    // containing all the resulting columns.
    case class InnerObjectConcat[+A <: SourceType](objects: TransSpec[A]*) extends TransSpec[A] //done

    case class OuterObjectConcat[+A <: SourceType](objects: TransSpec[A]*) extends TransSpec[A] //done

    case class ObjectDelete[+A <: SourceType](source: TransSpec[A], fields: Set[CPathField]) extends TransSpec[A]
    
    case class ArrayConcat[+A <: SourceType](arrays: TransSpec[A]*) extends TransSpec[A] //done
    
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
    
    // target is the transspec that provides defineedness information. The resulting table will be defined
    // and have the constant value wherever a row provided by the target transspec has at least one member
    // that is not undefined
    case class ConstLiteral[+A <: SourceType](value: CValue, target: TransSpec[A]) extends TransSpec[A]

    case class FilterDefined[+A <: SourceType](source: TransSpec[A], definedFor: TransSpec[A], definedness: Definedness) extends TransSpec[A]
  
    type TransSpec1 = TransSpec[Source1]

    object TransSpec {
      def mapSources[A <: SourceType, B <: SourceType](spec: TransSpec[A])(f: A => B): TransSpec[B] = {
        spec match {
          case Leaf(source) => Leaf(f(source))
          case trans.ConstLiteral(value, target) => trans.ConstLiteral(value, mapSources(target)(f))
          
          case trans.Filter(source, pred) => trans.Filter(mapSources(source)(f), mapSources(pred)(f))
          case trans.FilterDefined(source, definedFor, definedness) =>
            trans.FilterDefined(mapSources(source)(f), mapSources(definedFor)(f), definedness)
          
          case Scan(source, scanner) => Scan(mapSources(source)(f), scanner)
          
          case trans.Map1(source, f1) => trans.Map1(mapSources(source)(f), f1)
          case trans.Map2(left, right, f2) => trans.Map2(mapSources(left)(f), mapSources(right)(f), f2)
          
          case trans.OuterObjectConcat(objects @ _*) => trans.OuterObjectConcat(objects.map(mapSources(_)(f)): _*)
          case trans.InnerObjectConcat(objects @ _*) => trans.InnerObjectConcat(objects.map(mapSources(_)(f)): _*)
          case trans.ObjectDelete(source, fields) => trans.ObjectDelete(mapSources(source)(f), fields)
          case trans.ArrayConcat(arrays @ _*) => trans.ArrayConcat(arrays.map(mapSources(_)(f)): _*)
          
          case trans.WrapObject(source, field) => trans.WrapObject(mapSources(source)(f), field)
          case trans.WrapObjectDynamic(left, right) => trans.WrapObjectDynamic(mapSources(left)(f), mapSources(right)(f))
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
        case trans.ConstLiteral(value, target) => trans.ConstLiteral(value, deepMap(target)(f))
        
        case trans.Filter(source, pred) => trans.Filter(deepMap(source)(f), deepMap(pred)(f))
        case trans.FilterDefined(source, definedFor, definedness) => 
          trans.FilterDefined(deepMap(source)(f), deepMap(definedFor)(f), definedness)
        
        case Scan(source, scanner) => Scan(deepMap(source)(f), scanner)
        
        case trans.Map1(source, f1) => trans.Map1(deepMap(source)(f), f1)
        case trans.Map2(left, right, f2) => trans.Map2(deepMap(left)(f), deepMap(right)(f), f2)
        
        case trans.OuterObjectConcat(objects @ _*) => trans.OuterObjectConcat(objects.map(deepMap(_)(f)): _*)
        case trans.InnerObjectConcat(objects @ _*) => trans.InnerObjectConcat(objects.map(deepMap(_)(f)): _*)
        case trans.ObjectDelete(source, fields) => trans.ObjectDelete(deepMap(source)(f), fields)
        case trans.ArrayConcat(arrays @ _*) => trans.ArrayConcat(arrays.map(deepMap(_)(f)): _*)
        
        case trans.WrapObject(source, field) => trans.WrapObject(deepMap(source)(f), field)
        case trans.WrapObjectDynamic(source, right) => trans.WrapObjectDynamic(deepMap(source)(f), deepMap(right)(f))
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
      import constants._

      val Id = Leaf(Source)

      val DerefArray0 = DerefArrayStatic(Leaf(Source), CPathIndex(0))
      val DerefArray1 = DerefArrayStatic(Leaf(Source), CPathIndex(1))
      val DerefArray2 = DerefArrayStatic(Leaf(Source), CPathIndex(2))

      val PruneToKeyValue = InnerObjectConcat(
        WrapObject(SourceKey.Single, paths.Key.name), 
        WrapObject(SourceValue.Single, paths.Value.name))

      val DeleteKeyValue = ObjectDelete(Leaf(Source), Set(paths.Key, paths.Value))
    }
    
    type TransSpec2 = TransSpec[Source2]
    
    object TransSpec2 {
      val LeftId = Leaf(SourceLeft)
      val RightId = Leaf(SourceRight)

      def DerefArray0(source: Source2) = DerefArrayStatic(Leaf(source), CPathIndex(0))
      def DerefArray1(source: Source2) = DerefArrayStatic(Leaf(source), CPathIndex(1))
      def DerefArray2(source: Source2) = DerefArrayStatic(Leaf(source), CPathIndex(2))

      val DeleteKeyValueLeft = ObjectDelete(Leaf(SourceLeft), Set(paths.Key, paths.Value))
      val DeleteKeyValueRight = ObjectDelete(Leaf(SourceRight), Set(paths.Key, paths.Value))
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
    
    object constants {
      import paths._

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
}
