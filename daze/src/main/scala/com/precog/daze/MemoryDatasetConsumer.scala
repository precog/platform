package com.precog
package daze

import yggdrasil._
import yggdrasil.table._

import akka.dispatch.Await
import scalaz.Validation
import scalaz.effect.IO
import scalaz.iteratee._
import scalaz.syntax.monad._
import scalaz.std.set._
import Validation._
import Iteratee._

import blueeyes.json._

trait DatasetConsumersConfig extends EvaluatorConfig {
  def maxEvalDuration: akka.util.Duration
}

// TODO decouple this from the evaluator specifics
trait MemoryDatasetConsumer extends Evaluator with ColumnarTableModule with YggConfigComponent {
  import JsonAST._
  
  type X = Throwable
  type YggConfig <: DatasetConsumersConfig
  type SEvent = (Vector[Long], SValue)

  def consumeEval(userUID: String, graph: DepGraph, ctx: Context): Validation[X, Set[SEvent]] = {
    implicit val bind = Validation.validationMonad[Throwable]
    Validation.fromTryCatch {
      val result = eval(userUID, graph, ctx)
      
      val events = result.toJson collect {
        case JObject(JField("key", JArray(identities)) :: JField("value", value) :: Nil) =>
          (Vector(identities collect { case JInt(bi) => bi.toLong }: _*), jvalueToSValue(value))
      }
      
      events.toSet
    }
  }
  
  private def jvalueToSValue(value: JValue): SValue = value match {
    case JNothing => sys.error("don't use jnothing; doh!")
    case JField(_, _) => sys.error("seriously?!")
    case JNull => SNull
    case JBool(value) => SBoolean(value)
    case JInt(bi) => SDecimal(BigDecimal(bi))
    case JDouble(d) => SDecimal(d)
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
  
  def consumeEvalNotToSet(userUID: String, graph: DepGraph, ctx: Context) = {
    implicit val bind = Validation.validationMonad[Throwable]
    Validation.fromTryCatch {
      eval(userUID, graph, ctx).toJson
    } 
  }
}


// vim: set ts=4 sw=4 et:
