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

    val idSource = new IdSource {
      private val source = new java.util.concurrent.atomic.AtomicLong()
      def nextId() = source.getAndIncrement()
    }
  }

  def eval(query: String): M[Result] = {
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
            json <- table.toJson
          } yield json.size
        }
    }
  }
}




