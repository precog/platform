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
package ragnarok

import common.Path

import daze.{ Evaluator, EvaluatorConfig }

import yggdrasil.{ StorageModule, BaseConfig, IdSource }
import yggdrasil.{ Identities, SValue, SEvent }
import yggdrasil.util._
import yggdrasil.serialization._

import muspelheim.ParseEvalStack

import blueeyes.json._

import org.streum.configrity.Configuration

import akka.util.Duration

import scalaz._
import scalaz.syntax.monad._


trait PerfTestRunnerConfig extends BaseConfig with EvaluatorConfig {
  def optimize: Boolean
  def userUID: String
}

trait EvaluatingPerfTestRunner[M[+_], T] extends PerfTestRunner[M, T]
    with ParseEvalStack[M]
    with StorageModule[M]
    with IdSourceScannerModule[M] {

  type Result = Int

  type YggConfig <: PerfTestRunnerConfig

  trait EvaluatingPerfTestRunnerConfig extends PerfTestRunnerConfig {

    // TODO Get configuration from somewhere...
    val config = Configuration parse ""

    object valueSerialization extends SortSerialization[SValue] with SValueRunlengthFormatting with BinarySValueFormatting with ZippedStreamSerialization
    object eventSerialization extends SortSerialization[SEvent] with SEventRunlengthFormatting with BinarySValueFormatting with ZippedStreamSerialization
    object groupSerialization extends SortSerialization[(SValue, Identities, SValue)] with GroupRunlengthFormatting with BinarySValueFormatting with ZippedStreamSerialization
    object memoSerialization extends IncrementalSerialization[(Identities, SValue)] with SEventRunlengthFormatting with BinarySValueFormatting with ZippedStreamSerialization


    val maxEvalDuration: Duration = Duration(30, "seconds")
    
    val maxSliceSize = 10000

    val idSource = new IdSource {
      private val source = new java.util.concurrent.atomic.AtomicLong()
      def nextId() = source.getAndIncrement()
    }
  }

  def eval(query: String): M[Result] = try {
    val tree = compile(query)

    if (!tree.errors.isEmpty) {
      sys.error("Error parsing query:\n" + (tree.errors map (_.toString) mkString "\n"))
    }

    decorate(emit(tree)) match {
      case Left(stackError) =>
        sys.error("Failed to construct DAG.")

      case Right(dag) =>
        withContext { ctx =>
          for {
            table <- eval(yggConfig.userUID, dag, ctx, Path.Root, yggConfig.optimize)
            size <- table.renderJson(',').length
          } yield size
        }
    }
  } catch {
    case e: com.precog.quirrel.parser.Parser$ParseException =>
      sys.error("Error parsing query:\n\n%s\n\n%s" format (query, e.getMessage()))
  }
}




