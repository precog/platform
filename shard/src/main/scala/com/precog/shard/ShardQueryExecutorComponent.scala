package com.precog.shard

import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.util.Clock

import com.precog.common._
import com.precog.common.json._
import com.precog.common.security._

import com.precog.daze._
import com.precog.muspelheim._

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
    with EvaluatorConfig
    with IdSourceConfig {
  def clock: Clock
  val queryId = new java.util.concurrent.atomic.AtomicLong()
}


trait ShardQueryExecutorPlatform[M[+_]] extends Platform[M, StreamT[M, CharBuffer]] with ParseEvalStack[M] {
  case class StackException(error: StackError) extends Exception(error.toString)

  abstract class ShardQueryExecutor[N[+_]](N0: Monad[N])(implicit mn: M ~> N, nm: N ~> M) 
      extends Evaluator[N](N0) with QueryExecutor[N, StreamT[N, CharBuffer]] {

    type YggConfig <: ShardQueryExecutorConfig
    protected lazy val queryLogger = LoggerFactory.getLogger("com.precog.shard.ShardQueryExecutor")

    implicit val M: Monad[M]
    private implicit val N: Monad[N] = N0

    implicit def LineDecompose: Decomposer[instructions.Line] = new Decomposer[instructions.Line] {
      def decompose(line: instructions.Line): JValue = {
        JObject(JField("lineNum", JNum(line.line)), JField("colNum", JNum(line.col)), JField("detail", JString(line.text)))
      }
    }

    def execute(apiKey: String, query: String, prefix: Path, opts: QueryOptions): N[Validation[EvaluationError, StreamT[N, CharBuffer]]] = {
      val evaluationContext = EvaluationContext(apiKey, prefix, yggConfig.clock.now())
      val qid = yggConfig.queryId.getAndIncrement()
      queryLogger.info("Executing query %d for %s: %s, prefix: %s".format(qid, apiKey, query,prefix))

      import EvaluationError._

      val solution: Validation[Throwable, Validation[EvaluationError, StreamT[N, CharBuffer]]] = Validation.fromTryCatch {
        asBytecode(query) flatMap { bytecode =>
          ((systemError _) <-: (StackException(_)) <-: decorate(bytecode).disjunction.validation) flatMap { dag =>
            Validation.success(outputChunks(opts.output) {
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
            })
          }
        }
      }
      
      N.point {
        ((systemError _) <-: solution).flatMap(identity[Validation[EvaluationError, StreamT[N, CharBuffer]]])
      }
    }

    private def applyQueryOptions(opts: QueryOptions)(table: N[Table]): N[Table] = {
      import trans._

      def sort(table: N[Table]): N[Table] = if (!opts.sortOn.isEmpty) {
        val sortKey = InnerArrayConcat(opts.sortOn map { cpath =>
          WrapArray(cpath.nodes.foldLeft(constants.SourceValue.Single: TransSpec1) {
            case (inner, f @ CPathField(_)) =>
              DerefObjectStatic(inner, f)
            case (inner, i @ CPathIndex(_)) =>
              DerefArrayStatic(inner, i)
          })
        }: _*)

        table flatMap { tbl =>
          mn(tbl.sort(sortKey, opts.sortOrder))
        }
      } else {
        table
      }

      def page(table: N[Table]): N[Table] = opts.page map { case (offset, limit) =>
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

    private def outputChunks(output: QueryOutput)(tableM: N[Table]): StreamT[N, CharBuffer] =
      output match {
        case JsonOutput => jsonChunks(tableM)
        case CsvOutput => csvChunks(tableM)
      }

    private def csvChunks(tableM: N[Table]): StreamT[N, CharBuffer] = {
      import trans._
      val spec = DerefObjectStatic(Leaf(Source), TableModule.paths.Value)
      StreamT.wrapEffect(tableM.map(_.transform(spec).renderCsv.trans(mn)))
    }

    private def jsonChunks(tableM: N[Table]): StreamT[N, CharBuffer] = {
      import trans._

      StreamT.wrapEffect(
        tableM flatMap { table =>
          renderStream(table.transform(DerefObjectStatic(Leaf(Source), TableModule.paths.Value)))
        }
      )
    }

    private def renderStream(table: Table): N[StreamT[N, CharBuffer]] =
      N.point((CharBuffer.wrap("[") :: (table.renderJson(',').trans(mn))) ++ (CharBuffer.wrap("]") :: StreamT.empty[N, CharBuffer]))    
  }
}
// vim: set ts=4 sw=4 et:
