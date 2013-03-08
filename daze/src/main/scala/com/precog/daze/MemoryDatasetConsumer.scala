package com.precog.daze

import com.precog.common.Path
import com.precog.util.VectorCase
import com.precog.common.security._

import com.precog.yggdrasil._
import com.precog.yggdrasil.table._

import akka.dispatch.Await
import akka.util.Duration

import org.joda.time.DateTime

import scalaz._
import scalaz.Validation
import scalaz.effect.IO
import scalaz.syntax.monad._
import scalaz.syntax.copointed._
import scalaz.std.set._
import Validation._

import blueeyes.json._

trait MemoryDatasetConsumer[M[+_]] extends EvaluatorModule[M] {
  type IdType

  type X = Throwable
  type SEvent = (Vector[IdType], SValue)

  implicit def M: Monad[M] with Copointed[M]

  def Evaluator[N[+_]](N0: Monad[N])(implicit mn: M ~> N, nm: N ~> M): EvaluatorLike[N]
  
  def extractIds(jv: JValue): Seq[IdType]

  def consumeEval(apiKey: APIKey, graph: DepGraph, prefix: Path, optimize: Boolean = true): Validation[X, Set[SEvent]] = {
    val ctx = EvaluationContext(apiKey, prefix, new DateTime())
    Validation.fromTryCatch {
      implicit val nt = NaturalTransformation.refl[M]
      val evaluator = Evaluator(M)
      val result = evaluator.eval(graph, ctx, optimize)
      val json = result.flatMap(_.toJson).copoint filterNot { jvalue => {
        (jvalue \ "value") == JUndefined
      }}

      val events = json map { jvalue =>
        (Vector(extractIds(jvalue \ "key"): _*), jvalueToSValue(jvalue \ "value"))
      }
      
      val back = events.toSet
      evaluator.report.done.copoint
      back
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

trait LongIdMemoryDatasetConsumer[M[+_]] extends MemoryDatasetConsumer[M] {
  type IdType = Long
  def extractIds(jv: JValue): Seq[Long] = (jv --> classOf[JArray]).elements collect { case JNum(i) => i.toLong }
}

/**
  * String Identities are used for MongoDB collections, where the "_id" field 
  * represents the event identity. We still need to handle JNum entries in the
  * case of reductions.
  */
trait StringIdMemoryDatasetConsumer[M[+_]] extends MemoryDatasetConsumer[M] {
  type IdType = String
  // 
  def extractIds(jv: JValue): Seq[String] = 
    (jv --> classOf[JArray]).elements collect { 
      case JString(s) => s 
      case JNum(i)    => i.toString
    }
}

// vim: set ts=4 sw=4 et:
