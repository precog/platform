package com.precog.daze

import scalaz.Monoid

import scala.collection.mutable

import com.precog.common.json._
import com.precog.util._
import com.precog.yggdrasil._
import com.precog.yggdrasil.CLong

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
