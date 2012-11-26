package com.precog
package daze

import common.{ Path, VectorCase }
import common.security._

import yggdrasil._
import yggdrasil.table._

import akka.dispatch.Await
import akka.util.Duration

import scalaz._
import scalaz.Validation
import scalaz.effect.IO
import scalaz.syntax.monad._
import scalaz.syntax.copointed._
import scalaz.std.set._
import Validation._

import blueeyes.json._

// TODO decouple this from the evaluator specifics
trait MemoryDatasetConsumer[M[+_]] extends Evaluator[M] with TableModule[M] {
  type X = Throwable
  type SEvent = (Vector[Long], SValue)

  implicit def M: Monad[M] with Copointed[M]
  
  def consumeEval(apiKey: APIKey, graph: DepGraph, ctx: Context, prefix: Path, optimize: Boolean = true): Validation[X, Set[SEvent]] = {
    Validation.fromTryCatch {
      val result = eval(apiKey, graph, ctx, prefix, optimize)
      val json = result.flatMap(_.toJson).copoint filterNot { jvalue => {
        (jvalue \ "value") == JUndefined
      }}

      val events = json map { jvalue =>
        (Vector(((jvalue \ "key") --> classOf[JArray]).elements collect { case JNum(i) => i.toLong }: _*), jvalueToSValue(jvalue \ "value"))
      }
      
      events.toSet
    }
  }
  
  private def jvalueToSValue(value: JValue): SValue = value match {
    case JUndefined => sys.error("don't use jnothing; doh!")
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
