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
package com.precog.shard

import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.util.Clock

import com.precog.common._
import com.precog.common.json._
import com.precog.common.security._

import com.precog.daze._
import com.precog.muspelheim.ParseEvalStack

import com.precog.yggdrasil._
import com.precog.yggdrasil.actor._
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.serialization._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.util._

import org.slf4j.{ Logger, LoggerFactory }

import java.nio.CharBuffer

import scalaz._
import scalaz.Validation._
import scalaz.syntax.monad._
import scalaz.syntax.bifunctor._
import scalaz.syntax.std.either._

trait ShardQueryExecutorConfig
    extends BaseConfig
    with ColumnarTableModuleConfig
    with IdSourceConfig {
  def clock: Clock
  val queryId = new java.util.concurrent.atomic.AtomicLong()
}

trait ShardQueryExecutor[M[+_]] extends QueryExecutor[M, StreamT[M, CharBuffer]] with ParseEvalStack[M] {
  type YggConfig <: ShardQueryExecutorConfig

  protected lazy val queryLogger = LoggerFactory.getLogger("com.precog.shard.ShardQueryExecutor")

  case class StackException(error: StackError) extends Exception(error.toString)

  implicit def M: Monad[M]

  implicit def LineDecompose: Decomposer[instructions.Line] = new Decomposer[instructions.Line] {
    def decompose(line: instructions.Line): JValue = {
      JObject(JField("lineNum", JNum(line.line)) :: JField("colNum", JNum(line.col)) :: JField("detail", JString(line.text)) :: Nil)
    }
  }

  def execute(apiKey: String, query: String, prefix: Path, opts: QueryOptions): M[Validation[EvaluationError, StreamT[M, CharBuffer]]] = {
    val evaluationContext = EvaluationContext(apiKey, prefix, yggConfig.clock.now())
    val qid = yggConfig.queryId.getAndIncrement()
    queryLogger.info("[QID:%d] Executing query for %s: %s, prefix: %s".format(qid, apiKey, query, prefix))

    import EvaluationError._

    val solution: Validation[Throwable, Validation[EvaluationError, StreamT[M, CharBuffer]]] = Validation.fromTryCatch {
      asBytecode(query) flatMap { bytecode =>
        ((systemError _) <-: (StackException(_)) <-: decorate(bytecode).disjunction.validation) flatMap { dag =>
          Validation.success(outputChunks(opts.output) {
            applyQueryOptions(opts) {
              logger.debug("[QID:%d] Evaluating query".format(qid))
              if (queryLogger.isDebugEnabled) {
                eval(dag, evaluationContext, true) map {
                  _.logged(queryLogger, "[QID:"+qid+"]", "begin result stream", "end result stream") {
                    slice => "size: " + slice.size
                  }
                }
              } else {
                eval(dag, evaluationContext, true)
              }
            }
          })
        }
      }
    }
    
    M.point {
      ((systemError _) <-: solution).flatMap(identity[Validation[EvaluationError, StreamT[M, CharBuffer]]])
    }
  }

  private def applyQueryOptions(opts: QueryOptions)(table: M[Table]): M[Table] = {
    import trans._

    def sort(table: M[Table]): M[Table] = if (!opts.sortOn.isEmpty) {
      val sortKey = InnerArrayConcat(opts.sortOn map { cpath =>
        WrapArray(cpath.nodes.foldLeft(constants.SourceValue.Single: TransSpec1) {
          case (inner, f @ CPathField(_)) =>
            DerefObjectStatic(inner, f)
          case (inner, i @ CPathIndex(_)) =>
            DerefArrayStatic(inner, i)
        })
      }: _*)

      table flatMap (_.sort(sortKey, opts.sortOrder))
    } else {
      table
    }

    def page(table: M[Table]): M[Table] = opts.page map { case (offset, limit) =>
        table map (_.takeRange(offset, limit))
    } getOrElse table

    page(sort(table map (_.compact(constants.SourceValue.Single))))
  }

  private def asBytecode(query: String): Validation[EvaluationError, Vector[Instruction]] = {
    try {
      val forest = compile(query)
      val validForest = forest filter { _.errors.isEmpty }
      
      if (validForest.size == 1) {
        success(emit(validForest.head))
      } else if (validForest.size > 1) {
        failure(UserError(
          JArray(
            JArray(
              JObject(
                JField("message", JString("Ambiguous parse results.")) :: Nil) :: Nil) :: Nil)))
      } else {
        val nested = forest map { tree =>
          JArray(
            (tree.errors: Set[Error]) map { err =>
              val loc = err.loc
              val tp = err.tp

              JObject(
                JField("message", JString("Errors occurred compiling your query."))
                  :: JField("line", JString(loc.line))
                  :: JField("lineNum", JNum(loc.lineNum))
                  :: JField("colNum", JNum(loc.colNum))
                  :: JField("detail", JString(tp.toString))
                  :: Nil)
            } toList)
        }
        
        failure(UserError(JArray(nested.toList)))
      }
    } catch {
      case ex: ParseException => failure(
        UserError(
          JArray(
            JObject(
              JField("message", JString("An error occurred parsing your query."))
                :: JField("line", JString(ex.failures.head.tail.line))
                :: JField("lineNum", JNum(ex.failures.head.tail.lineNum))
                :: JField("colNum", JNum(ex.failures.head.tail.colNum))
                :: JField("detail", JString(ex.mkString))
                :: Nil
            ) :: Nil
          )
        )
      )
    }
  }

  private def outputChunks(output: QueryOutput)(tableM: M[Table]): StreamT[M, CharBuffer] =
    output match {
      case JsonOutput => jsonChunks(tableM)
      case CsvOutput => csvChunks(tableM)
    }

  private def csvChunks(tableM: M[Table]): StreamT[M, CharBuffer] = {
    import trans._
    val spec = DerefObjectStatic(Leaf(Source), TableModule.paths.Value)
    StreamT.wrapEffect(tableM.map(_.transform(spec).renderCsv))
  }

  private def jsonChunks(tableM: M[Table]): StreamT[M, CharBuffer] = {
    import trans._

    StreamT.wrapEffect(
      tableM flatMap { table =>
        renderStream(table.transform(DerefObjectStatic(Leaf(Source), TableModule.paths.Value)))
      }
    )
  }

  private def renderStream(table: Table): M[StreamT[M, CharBuffer]] =
    M.point((CharBuffer.wrap("[") :: (table renderJson ',')) ++ (CharBuffer.wrap("]") :: StreamT.empty[M, CharBuffer]))    
}
// vim: set ts=4 sw=4 et:
