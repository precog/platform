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
import blueeyes.util.Clock

import com.precog.accounts.BasicAccountManager
import com.precog.common._
import com.precog.common.json._
import com.precog.common.security._

import com.precog.daze._
import com.precog.muspelheim.ParseEvalStack

import com.precog.yggdrasil._
import com.precog.yggdrasil.actor._
import com.precog.yggdrasil.jdbm3._
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.serialization._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.table.jdbm3._
import com.precog.yggdrasil.util._

import com.precog.util.FilesystemFileOps

import akka.actor.ActorSystem
import akka.dispatch._
import akka.pattern.ask
import akka.util.duration._
import akka.util.Duration
import akka.util.Timeout

import org.slf4j.{LoggerFactory, MDC}

import java.io.File
import java.nio.CharBuffer
import java.util.concurrent.{ConcurrentHashMap, Executors}

import scalaz._
import scalaz.Validation._
import scalaz.effect.IO
import scalaz.syntax.monad._
import scalaz.syntax.bifunctor._
import scalaz.syntax.std.either._

import org.streum.configrity.Configuration

trait ShardQueryExecutorFactory
    extends QueryExecutorFactory[Future]
    with IdSourceScannerModule[Future] { self =>

  protected lazy val queryLogger = LoggerFactory.getLogger("com.precog.shard.ShardQueryExecutor")
  protected val queryId = new java.util.concurrent.atomic.AtomicLong

  def clock: Clock

  def accountManager: BasicAccountManager[Future]

  def defaultAsyncContext: ExecutionContext

  private val executorCache = new ConcurrentHashMap[AccountId, ExecutionContext]()
  
  private def asyncContextFor(accountId: AccountId): ExecutionContext = {
    if (executorCache.contains(accountId)) {
      executorCache.get(accountId)
    } else {
      // FIXME: Dummy pool for now
      executorCache.putIfAbsent(accountId, ExecutionContext.fromExecutor(Executors.newCachedThreadPool()))
    }
  }

  def status(): Future[Validation[String, JValue]] = {
    Promise.successful(Failure("Status not supported yet"))(defaultAsyncContext)
  }

  def executorFor(apiKey: APIKey) = {
    accountManager.listAccountIds(apiKey) map { accounts =>
      if (accounts.size < 1) {
        Failure("Could not locate accountId for apiKey " + apiKey)
      } else {
        Success(newExecutor(asyncContextFor(accounts.head))) // FIXME: Which account should we use if there's more than one?
      }
    }
  }

  protected def newExecutor(asyncContext: ExecutionContext): QueryExecutor[Future]

  trait ShardQueryExecutor[M[+_]] extends QueryExecutor[M] with ParseEvalStack[M] {
    import scalaz.syntax.monad._

    case class StackException(error: StackError) extends Exception(error.toString)

    def execute(userUID: String, query: String, prefix: Path, opts: QueryOptions): Validation[EvaluationError, StreamT[M, CharBuffer]] = {
      val evaluationContext = EvaluationContext(apiKey, prefix, clock.now())
      val qid = queryId.getAndIncrement
      queryLogger.info("Executing query %d for %s: %s, prefix: %s".format(qid, userUID, query,prefix))

      import EvaluationError._

      val solution: Validation[Throwable, Validation[EvaluationError, StreamT[M, CharBuffer]]] = Validation.fromTryCatch {
        asBytecode(query) flatMap { bytecode =>
          ((systemError _) <-: (StackException(_)) <-: decorate(bytecode).disjunction.validation) flatMap { dag =>
            Validation.success(jsonChunks(withContext { ctx =>
              applyQueryOptions(opts) {
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
            }))
          }
        }
      }
      
      ((systemError _) <-: solution).flatMap(identity[Validation[EvaluationError, StreamT[M, CharBuffer]]])
    }

    private def applyQueryOptions(opts: QueryOptions)(table: M[Table]): M[Table] = {
      import trans._

      def sort(table: M[Table]): M[Table] = if (!opts.sortOn.isEmpty) {
        val sortKey = ArrayConcat(opts.sortOn map { cpath =>
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
}
// vim: set ts=4 sw=4 et:
