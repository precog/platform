/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
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

  implicit def coM: Copointed[M]
  
  def consumeEval(userUID: String, graph: DepGraph, ctx: Context, prefix: Path, optimize: Boolean = true): Validation[X, Set[SEvent]] = {
    implicit val bind = Validation.validationMonad[Throwable]
    Validation.fromTryCatch {
      val result = eval(userUID, graph, ctx, prefix, optimize)
      val json = result.flatMap(_.toJson).copoint filterNot { jvalue =>
        (jvalue \ "value") == JNothing
      }
      
      val events = json map { jvalue =>
        (VectorCase(((jvalue \ "key") --> classOf[JArray]).elements collect { case JInt(i) => i.toLong }: _*), jvalueToSValue(jvalue \ "value"))
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
}


// vim: set ts=4 sw=4 et:
