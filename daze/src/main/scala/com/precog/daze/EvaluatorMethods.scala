package com.precog.daze

import scalaz.Monoid

import scala.collection.mutable

import com.precog.common.json._
import com.precog.util._
import com.precog.yggdrasil._
import com.precog.yggdrasil.TableModule.paths

import blueeyes.json._

import scalaz.std.map._

trait EvaluatorMethodsModule[M[+_]] extends DAG with TableModule[M] with TableLibModule[M] with OpFinderModule[M] {
  import dag._ 
  import instructions._
  import library._
  import trans._
  import trans.constants._

  trait EvaluatorMethods extends OpFinder {
    type TableTransSpec[+A <: SourceType] = Map[CPathField, TransSpec[A]]
    type TableTransSpec1 = TableTransSpec[Source1]
    type TableTransSpec2 = TableTransSpec[Source2]

    def jValueToCValue(jvalue: JValue): Option[CValue] = jvalue match {
      case JString(s) => Some(CString(s))
      case JNumLong(l) => Some(CLong(l))
      case JNumDouble(d) => Some(CDouble(d))
      case JNumBigDec(d) => Some(CNum(d))
      case JBool(b) => Some(CBoolean(b))
      case JNull => Some(CNull)
      case JUndefined => Some(CUndefined)
      case JObject.empty => Some(CEmptyObject)
      case JArray.empty => Some(CEmptyArray)
      case _ => None
    }
    
    def transJValue[A <: SourceType](jvalue: JValue, target: TransSpec[A]): TransSpec[A] = {
      jValueToCValue(jvalue) map { cvalue =>
        trans.ConstLiteral(cvalue, target)
      } getOrElse {
        jvalue match {
          case JArray(elements) => InnerArrayConcat(elements map {
            element => trans.WrapArray(transJValue(element, target))
          }: _*)
          case JObject(fields) => InnerObjectConcat(fields.toSeq map {
            case (key, value) => trans.WrapObject(transJValue(value, target), key)
          }: _*)
          case _ =>
            sys.error("Can't handle JType")
        }
      }
    }

    def transFromBinOp[A <: SourceType](op: BinaryOperation, ctx: EvaluationContext)(left: TransSpec[A], right: TransSpec[A]): TransSpec[A] = op match {
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

    def makeTableTrans(tableTrans: TableTransSpec1): TransSpec1 = {
      val wrapped = for ((key @ CPathField(fieldName), value) <- tableTrans) yield {
        val mapped = TransSpec.deepMap(value) {
          case Leaf(_) => DerefObjectStatic(Leaf(Source), key)
        }
        
        trans.WrapObject(mapped, fieldName)
      }
      
      wrapped.foldLeft[TransSpec1](ObjectDelete(Leaf(Source), Set(tableTrans.keys.toSeq: _*))) { (acc, ts) =>
        trans.InnerObjectConcat(acc, ts)
      }
    }

    def liftToValues(trans: TransSpec1): TransSpec1 =
      makeTableTrans(Map(paths.Value -> trans))

    def combineTransSpecs(specs: List[TransSpec1]): TransSpec1 =
      specs map { trans.WrapArray(_): TransSpec1 } reduceOption { trans.OuterArrayConcat(_, _) } get

    //TODO don't use Map1, returns an empty array of type CNum
    def buildConstantWrapSpec[A <: SourceType](source: TransSpec[A]): TransSpec[A] = {  
      val bottomWrapped = trans.WrapObject(trans.ConstLiteral(CEmptyArray, source), paths.Key.name)
      trans.InnerObjectConcat(bottomWrapped, trans.WrapObject(source, paths.Value.name))
    }

    def buildValueWrapSpec[A <: SourceType](source: TransSpec[A]): TransSpec[A] = {
      trans.WrapObject(source, paths.Value.name)
    }
    
    def buildJoinKeySpec(sharedLength: Int): TransSpec1 = {
      val components = for (i <- 0 until sharedLength)
        yield trans.WrapArray(DerefArrayStatic(SourceKey.Single, CPathIndex(i))): TransSpec1

      components reduce { trans.InnerArrayConcat(_, _) }
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
        derefs reduce { trans.InnerArrayConcat(_, _) }
      
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
      
      val wrappedValueSpec = trans.WrapObject(spec(leftValueSpec, rightValueSpec), paths.Value.name)

      InnerObjectConcat(wrappedIdentitySpec, wrappedValueSpec)
    }
    
    def buildIdShuffleSpec(indexes: Vector[Int]): TransSpec1 = {
      indexes map { idx =>
        trans.WrapArray(DerefArrayStatic(Leaf(Source), CPathIndex(idx))): TransSpec1
      } reduce { trans.InnerArrayConcat(_, _) }
    }
  }
}


// vim: set ts=4 sw=4 et:
