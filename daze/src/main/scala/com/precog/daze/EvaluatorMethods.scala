package com.precog.daze

import scalaz.Monoid

import scala.collection.mutable

import com.precog.common._
import com.precog.util._
import com.precog.yggdrasil._
import com.precog.yggdrasil.TableModule.paths

import scalaz.std.map._

trait EvaluatorMethodsModule[M[+_]] extends DAG with TableModule[M] with TableLibModule[M] with OpFinderModule[M] {
  import dag._ 
  import instructions._
  import library._
  import trans._
  import trans.constants._

  trait EvaluatorMethods extends OpFinder {
    def MorphContext(ctx: EvaluationContext, node: DepGraph): MorphContext

    def rValueToCValue(rvalue: RValue): Option[CValue] = rvalue match {
      case cvalue: CValue => Some(cvalue)
      case RArray.empty => Some(CEmptyArray)
      case RObject.empty => Some(CEmptyObject)
      case _ => None
    }
    
    def transRValue[A <: SourceType](rvalue: RValue, target: TransSpec[A]): TransSpec[A] = {
      rValueToCValue(rvalue) map { cvalue =>
        trans.ConstLiteral(cvalue, target)
      } getOrElse {
        rvalue match {
          case RArray(elements) => InnerArrayConcat(elements map {
            element => trans.WrapArray(transRValue(element, target))
          }: _*)
          case RObject(fields) => InnerObjectConcat(fields.toSeq map {
            case (key, value) => trans.WrapObject(transRValue(value, target), key)
          }: _*)
          case _ =>
            sys.error("Can't handle RValue")
        }
      }
    }

    def transFromBinOp[A <: SourceType](op: BinaryOperation, ctx: MorphContext)(left: TransSpec[A], right: TransSpec[A]): TransSpec[A] = op match {
      case Eq => trans.Equal[A](left, right)
      case NotEq => op1ForUnOp(Comp).spec(ctx)(trans.Equal[A](left, right))
      case instructions.WrapObject => WrapObjectDynamic(left, right)
      case JoinObject => InnerObjectConcat(left, right)
      case JoinArray => InnerArrayConcat(left, right)
      case instructions.ArraySwap => sys.error("nothing happens")
      case DerefObject => DerefObjectDynamic(left, right)
      case DerefMetadata => sys.error("cannot do a dynamic metadata deref")
      case DerefArray => DerefArrayDynamic(left, right)
      case _ => op2ForBinOp(op).get.spec(ctx)(left, right)
    }

    def combineTransSpecs(specs: List[TransSpec1]): TransSpec1 =
      specs map { trans.WrapArray(_): TransSpec1 } reduceLeftOption { trans.OuterArrayConcat(_, _) } get

    def buildJoinKeySpec(sharedLength: Int): TransSpec1 = {
      val components = for (i <- 0 until sharedLength)
        yield trans.WrapArray(DerefArrayStatic(SourceKey.Single, CPathIndex(i))): TransSpec1

      components reduceLeft { trans.InnerArrayConcat(_, _) }
    }
    
    def buildWrappedJoinSpec(sharedLength: Int, leftLength: Int, rightLength: Int)(spec: (TransSpec2, TransSpec2) => TransSpec2): TransSpec2 = {
      val leftIdentitySpec = DerefObjectStatic(Leaf(SourceLeft), paths.Key)
      val rightIdentitySpec = DerefObjectStatic(Leaf(SourceRight), paths.Key)
      
      val sharedDerefs = for (i <- 0 until sharedLength)
        yield trans.WrapArray(DerefArrayStatic(leftIdentitySpec, CPathIndex(i)))
      
      val unsharedLeft = for (i <- sharedLength until leftLength)
        yield trans.WrapArray(DerefArrayStatic(leftIdentitySpec, CPathIndex(i)))
      
      val unsharedRight = for (i <- sharedLength until rightLength)
        yield trans.WrapArray(DerefArrayStatic(rightIdentitySpec, CPathIndex(i)))
      
      val derefs: Seq[TransSpec2] = sharedDerefs ++ unsharedLeft ++ unsharedRight
      
      val newIdentitySpec = if (derefs.isEmpty)
        trans.ConstLiteral(CEmptyArray, Leaf(SourceLeft))
      else
        derefs reduceLeft { trans.InnerArrayConcat(_, _) }
      
      val wrappedIdentitySpec = trans.WrapObject(newIdentitySpec, paths.Key.name)
      
      val leftValueSpec = DerefObjectStatic(Leaf(SourceLeft), paths.Value)
      val rightValueSpec = DerefObjectStatic(Leaf(SourceRight), paths.Value)

      val wrappedValueSpec = trans.WrapObject(spec(leftValueSpec, rightValueSpec), paths.Value.name)
        
      InnerObjectConcat(wrappedValueSpec, wrappedIdentitySpec)
    }
    
    def buildWrappedCrossSpec(spec: (TransSpec2, TransSpec2) => TransSpec2): TransSpec2 = {
      val leftIdentitySpec = DerefObjectStatic(Leaf(SourceLeft), paths.Key)
      val rightIdentitySpec = DerefObjectStatic(Leaf(SourceRight), paths.Key)
      
      val newIdentitySpec = InnerArrayConcat(leftIdentitySpec, rightIdentitySpec)
      
      val wrappedIdentitySpec = trans.WrapObject(newIdentitySpec, paths.Key.name)

      val leftValueSpec = DerefObjectStatic(Leaf(SourceLeft), paths.Value)
      val rightValueSpec = DerefObjectStatic(Leaf(SourceRight), paths.Value)

      val valueSpec = spec(leftValueSpec, rightValueSpec)
      val wrappedValueSpec = trans.WrapObject(valueSpec, paths.Value.name)

      InnerObjectConcat(wrappedIdentitySpec, wrappedValueSpec)
    }
    
    def buildIdShuffleSpec(indexes: Vector[Int]): TransSpec1 = {
      indexes map { idx =>
        trans.WrapArray(DerefArrayStatic(Leaf(Source), CPathIndex(idx))): TransSpec1
      } reduceLeft { trans.InnerArrayConcat(_, _) }
    }
  }
}


// vim: set ts=4 sw=4 et:
