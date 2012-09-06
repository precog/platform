package com.precog
package daze

import common.VectorCase

import yggdrasil._
import yggdrasil.table._

import akka.dispatch.Await
import akka.util.Duration

import com.precog.common.Path

import scalaz._
import scalaz.Validation
import scalaz.effect.IO
import scalaz.syntax.monad._
import scalaz.syntax.copointed._
import scalaz.std.set._
import Validation._

import blueeyes.json._

trait DatasetConsumersConfig extends EvaluatorConfig {
  def maxEvalDuration: Duration
}

// TODO decouple this from the evaluator specifics
trait MemoryDatasetConsumer[M[+_]] extends Evaluator[M] with TableModule[M] with YggConfigComponent {
  import JsonAST._
  
  type X = Throwable
  type YggConfig <: DatasetConsumersConfig
  type SEvent = (VectorCase[Long], SValue)

  implicit def M: Monad[M] with Copointed[M]
  
  def consumeEval(userUID: String, graph: DepGraph, ctx: Context, prefix: Path, optimize: Boolean = true): Validation[X, Set[SEvent]] = {
    Validation.fromTryCatch {
      val result = eval(userUID, graph, ctx, prefix, optimize)
      val json = result.flatMap(_.toJson).copoint filterNot { jvalue => {
        (jvalue \ "value") == JNothing
      }}

      val events = json map { jvalue =>
        (VectorCase(((jvalue \ "key") --> classOf[JArray]).elements collect { case JNum(i) => i.toLong }: _*), jvalueToSValue(jvalue \ "value"))
      }
      
      events.toSet
    }
  }
  
  private def jvalueToSValue(value: JValue): SValue = value match {
    case JNothing => sys.error("don't use jnothing; doh!")
    case JField(_, _) => sys.error("seriously?!")
    case JNull => SNull
    case JBool(value) => SBoolean(value)
    case JNum(bi) => SDecimal(bi)
    case JString(str) => SString(str)
    
    case JObject(fields) => {
      val map: Map[String, SValue] = fields.map({
        case JField(name, value) => (name, jvalueToSValue(value))
      })(collection.breakOut)
      
      SObject(map)
    }
    
    case JArray(values) =>
      SArray(Vector(values map jvalueToSValue: _*))
  }
}


// vim: set ts=4 sw=4 et:
