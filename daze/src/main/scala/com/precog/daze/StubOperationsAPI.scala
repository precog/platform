package com.precog.daze

import com.precog.yggdrasil._
import com.precog.yggdrasil.util.Enumerators
import com.precog.analytics.Path

import akka.dispatch.Future

import blueeyes.json._
import JsonAST._

import scala.io.Source

import scalaz._
import scalaz.std.AllInstances._
import scalaz.effect._
import scalaz.iteratee._
import Function._
import IterateeT._

object StubOperationsAPI {
  import akka.actor.ActorSystem
  import akka.dispatch.ExecutionContext

  val actorSystem = ActorSystem("stub_operations_api")
  implicit val actorExecutionContext = ExecutionContext.defaultExecutionContext(actorSystem)
}

// TODO decouple this from the evaluator specifics
trait DatasetConsumers extends Evaluator {
  protected def consumeEval(graph: DepGraph): Set[SEvent] = 
    ((consume[Unit, SEvent, IO, Set] &= eval(graph).enum[IO]) run { err => sys.error("O NOES!!!") }) unsafePerformIO
}

trait StubOperationsAPI extends OperationsAPI with DatasetConsumers with DefaultYggConfig {
  import StubOperationsAPI._
  
  override object ops extends DatasetEnumOps {
    def sort[X](enum: DatasetEnum[X, SEvent, IO])(implicit order: Order[SEvent]): DatasetEnum[X, SEvent, IO] = {
      DatasetEnum(Enumerators.sort[X](enum.enum, yggdrasilConfig.sortBufferSize, yggdrasilConfig.workDir, enum.descriptor))
    }
  }

  override object query extends StorageEngineQueryAPI {
    private var pathIds = Map[Path, Int]()
    private var currentId = 0
    
    def fullProjection[X](path: Path): Future[DatasetEnum[X, SEvent, IO]] =
      akka.dispatch.Promise.successful(DatasetEnum(readJSON[X](path)))
    
    private def readJSON[X](path: Path) = {
      val src = Source.fromInputStream(getClass getResourceAsStream path.elements.mkString("/", "/", ".json"))
      val stream = Stream from 0 map scaleId(path) zip (src.getLines map parseJSON toStream) map tupled(wrapSEvent)
      Iteratee.enumPStream[X, SEvent, IO](stream)
    }
    
    private def scaleId(path: Path)(seed: Int): Long = {
      val scalar = synchronized {
        if (!(pathIds contains path)) {
          pathIds += (path -> currentId)
          currentId += 1
        }
        
        pathIds(path)
      }
      
      (scalar.toLong << 32) | seed
    }
    
    private def parseJSON(str: String): JValue =
      JsonParser parse str
    
    private def wrapSEvent(id: Long, value: JValue): SEvent =
      (Vector(id), wrapSValue(value))
    
    private def wrapSValue(value: JValue): SValue = new SValue {
      def fold[A](
          obj: Map[String, SValue] => A,
          arr: Vector[SValue] => A,
          str: String => A,
          bool: Boolean => A,
          long: Long => A,
          double: Double => A,
          num: BigDecimal => A,
          nul: => A): A = value match {
            
        case JObject(fields) => {
          val pairs = fields map {
            case JField(key, value) => (key, wrapSValue(value))
          }
          
          obj(Map(pairs: _*))
        }
        
        case JArray(values) => arr(Vector(values map wrapSValue: _*))
        
        case JString(s) => str(s)
        
        case JBool(b) => bool(b)
        
        case JInt(i) => num(BigDecimal(i.toString))
        
        case JDouble(d) => num(d)
        
        case JNull => nul
        
        case JNothing => sys.error("Hit JNothing")
      }
    }
  }
}

// vim: set ts=4 sw=4 et:
