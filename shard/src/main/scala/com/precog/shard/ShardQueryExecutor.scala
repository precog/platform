package com.precog.shard

import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.util.Clock

import com.precog.common._
import com.precog.common.json._
import com.precog.common.security._
import com.precog.common.accounts._
import com.precog.daze._
import com.precog.muspelheim._

import com.precog.yggdrasil._
import com.precog.yggdrasil.actor._
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.serialization._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.util._
import com.precog.util.FilesystemFileOps

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

case class FaultPosition(line: Int, col: Int, text: String)

object FaultPosition {
  import shapeless._
  import blueeyes.json.serialization._
  import DefaultSerialization.{ DateTimeExtractor => _, DateTimeDecomposer => _, _ }

  implicit val iso = Iso.hlist(FaultPosition.apply _, FaultPosition.unapply _)
  val schema = "line" :: "column" :: "text" :: HNil
  implicit val (decomposer, extractor) = IsoSerialization.serialization[FaultPosition](schema)
}

sealed trait Fault {
  def pos: Option[FaultPosition]
  def message: String
}

object Fault {
  case class Warning(pos: Option[FaultPosition], message: String) extends Fault
  case class Error(pos: Option[FaultPosition], message: String) extends Fault
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
    
    lazy val report = queryReport contramap { (l: instructions.Line) =>
      Option(FaultPosition(l.line, l.col, l.text))
    }

    def queryReport: QueryLogger[N, Option[FaultPosition]] 

    def execute(apiKey: String, query: String, prefix: Path, opts: QueryOptions): N[Validation[EvaluationError, StreamT[N, CharBuffer]]] = {
      val evaluationContext = EvaluationContext(apiKey, prefix, yggConfig.clock.now())
      val qid = yggConfig.queryId.getAndIncrement()
      queryLogger.info("[QID:%d] Executing query for %s: %s, prefix: %s".format(qid, apiKey, query, prefix))

      import EvaluationError._

      val solution: Validation[Throwable, Validation[EvaluationError, StreamT[N, CharBuffer]]] = Validation.fromTryCatch {
        val (faults, bytecode) = asBytecode(query) 
        
        val resultVN: Validation[EvaluationError, N[Table]] = bytecode map { instrs =>
          ((systemError _) <-: (StackException(_)) <-: decorate(instrs).disjunction.validation) map { dag =>
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
          }
        } getOrElse {
          // compilation errors will be reported as warnings, but there are no results so 
          // we just return an empty stream as the success
          success(N.point(Table.empty))
        }

        resultVN map { nt => 
          val faultN = faults map {
            case Fault.Error(pos, msg) => queryReport.warn(pos, msg) 
            case Fault.Warning(pos, msg) => queryReport.warn(pos, msg)
          } reduceOption { _ >> _ } getOrElse (N point ())

          val chunks = outputChunks(opts.output) {
            faultN >> nt
          }
          
          val effect = queryReport.done map { _ =>
            StreamT.empty[N, CharBuffer]
          }
          
          chunks ++ (StreamT wrapEffect effect)
        }
      } 
      
      N point {
        ((systemError _) <-: solution) flatMap {
          identity[Validation[EvaluationError, StreamT[N, CharBuffer]]]
        }
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

      def page(table: N[Table]): N[Table] = opts.page map {
        case (offset, limit) =>
          table map { _.takeRange(offset, limit) }
      } getOrElse table

      page(sort(table map (_.compact(constants.SourceValue.Single))))
    }

    private def asBytecode(query: String): (Set[Fault], Option[Vector[Instruction]]) = {
      def renderError(err: Error): Fault = {
        val loc = err.loc
        val tp = err.tp

        val constr = if (isWarning(err)) Fault.Warning.apply _ else Fault.Error.apply _

        constr(Some(FaultPosition(loc.lineNum, loc.colNum, loc.line)), tp.toString)
      }
      
      try {
        val forest = compile(query)
        val validForest = forest filter { tree =>
          tree.errors forall isWarning
        }
        
        if (validForest.size == 1) {
          val tree = validForest.head
          val faults = tree.errors map renderError
          
          (faults, Some(emit(validForest.head)))
        } else if (validForest.size > 1) {
          (Set(Fault.Error(None, "Ambiguous parse results.")), None)
        } else {
          val faults = forest flatMap { tree =>
            (tree.errors: Set[Error]) map renderError
          }

          (faults, None)
        }
      } catch {
        case ex: ParseException =>
          val loc = ex.failures.head.tail
          val fault = Fault.Error(Some(FaultPosition(loc.lineNum, loc.colNum, loc.line)), ex.mkString)

          (Set(fault), None)
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
