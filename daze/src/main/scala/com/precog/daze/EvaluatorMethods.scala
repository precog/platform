package com.precog.daze

import scalaz.Monoid

import scala.collection.mutable

import com.precog.common.json._
import com.precog.util._
import com.precog.yggdrasil._
import com.precog.yggdrasil.CLong

import blueeyes.json._

import scalaz.std.map._

trait EvaluatorMethodsModule[M[+_]] extends DAG with OpFinderModule[M] {
  import library._
  import trans._

  trait EvaluatorMethods extends OpFinder {
    import dag._ 
    import instructions._

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
      case _ => trans.Map2(left, right, op2ForBinOp(op).get.f2(ctx))     // if this fails, we're missing a case above
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
  }
}


// vim: set ts=4 sw=4 et:
