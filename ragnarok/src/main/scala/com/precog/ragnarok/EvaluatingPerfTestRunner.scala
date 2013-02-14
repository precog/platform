package com.precog
package ragnarok

import common.Path
import common.security._

import daze.{ EvaluatorModule, EvaluationContext }

import yggdrasil._
import yggdrasil.table.ColumnarTableModuleConfig
import yggdrasil.util._
import yggdrasil.serialization._

import muspelheim.ParseEvalStack

import blueeyes.json._

import org.streum.configrity.Configuration

import akka.util.Duration

import scalaz._
import scalaz.syntax.monad._


trait PerfTestRunnerConfig extends BaseConfig
    with IdSourceConfig
    with ColumnarTableModuleConfig {
    
  def optimize: Boolean
  def apiKey: APIKey
}

trait EvaluatingPerfTestRunnerConfig extends PerfTestRunnerConfig {

  // TODO Get configuration from somewhere...
  val config = Configuration parse ""

  val maxEvalDuration: Duration = Duration(30, "seconds")
  
  val maxSliceSize = 10000
  val smallSliceSize = 8

  val idSource = new FreshAtomicIdSource
}


trait EvaluatingPerfTestRunner[M[+_], T] extends ParseEvalStack[M]
    with StorageModule[M]
    with IdSourceScannerModule
    with PerfTestRunner[M, T] {

  type Result = Int

  type YggConfig <: PerfTestRunnerConfig

  private implicit val nt = NaturalTransformation.refl[M]
  def Evaluator[N[+_]](N0: Monad[N])(implicit mn: M ~> N, nm: N ~> M): EvaluatorLike[N]

  def eval(query: String): M[Result] = try {
    val forest = compile(query)
    val valid = forest filter { _.errors.isEmpty }

    if (valid.isEmpty) {
      sys.error("Error parsing query:\n" + (forest flatMap { _.errors } map { _.toString } mkString "\n"))
    } else if (valid.size > 1) {
      sys.error("Ambiguous parse tree.")
    }
    
    val tree = valid.head

    decorate(emit(tree)) match {
      case Left(stackError) =>
        sys.error("Failed to construct DAG.")

      case Right(dag) =>
        for {
          table <- Evaluator(M).eval(dag, EvaluationContext(yggConfig.apiKey, Path.Root, new org.joda.time.DateTime()), yggConfig.optimize)
          size <- countStream(table.renderJson(','))
        } yield size
    }
  } catch {
    case e: com.precog.quirrel.parser.Parser$ParseException =>
      sys.error("Error parsing query:\n\n%s\n\n%s" format (query, e.getMessage()))
  }
  
  private def countStream[A](str: StreamT[M, A]): M[Int] = {
    for {
      optTail <- str.uncons
      res = optTail map { _._2 } map { tail => countStream(tail) map (1 +) }
      
      back <- res getOrElse M.point(0)
    } yield back
  }
}

