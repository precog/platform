package com.precog.daze

import com.precog.common.{Path, VectorCase}
import com.precog.yggdrasil._

import akka.dispatch.Await
import akka.dispatch.ExecutionContext
import akka.dispatch.Future
import akka.util.duration._

import blueeyes.json.JsonAST._
import blueeyes.json.JsonParser

import scala.io.Source

import scalaz._
import scalaz.std.AllInstances._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.syntax.monad._
import Function._
import IterateeT._
import Validation._

object StubOperationsAPI {
  import akka.actor.ActorSystem
  import akka.dispatch.ExecutionContext

  val actorSystem = ActorSystem("stub_operations_api")
  implicit val asyncContext: akka.dispatch.ExecutionContext = ExecutionContext.defaultExecutionContext(actorSystem)
}

trait StubOperationsAPI 
    extends StorageEngineQueryComponent
    with IterableDatasetOpsComponent
    with MemoryDatasetConsumer { self =>
  type YggConfig <: DatasetConsumersConfig with EvaluatorConfig with YggEnumOpsConfig
  type Dataset[E] = IterableDataset[E]

  implicit def asyncContext = StubOperationsAPI.asyncContext
  
  object query extends QueryAPI {
    val chunkSize = 2000
  }
  
  trait QueryAPI extends StorageEngineQueryAPI[Dataset] {
    private var pathIds = Map[Path, Int]()
    private var currentId = 0

    def chunkSize: Int
    
    private case class StubDatasetMask(userUID: String, path: Path, selector: Vector[Either[Int, String]]) extends DatasetMask[Dataset] {
      def derefObject(field: String): DatasetMask[Dataset] = copy(selector = selector :+ Right(field))
      def derefArray(index: Int): DatasetMask[Dataset] = copy(selector = selector :+ Left(index))
      def typed(tpe: SType): DatasetMask[Dataset] = this
      
      def realize(expiresAt: Long): Dataset[SValue] = {
        fullProjection(userUID, path, expiresAt) collect unlift(mask)
      }
      
      private def mask(sv: SValue): Option[SValue] = {
        selector.foldLeft(Some(sv): Option[SValue]) {
          case (None, _) => None
          case (Some(SObject(obj)), Right(field)) => obj get field
          case (Some(SArray(arr)),  Left(index)) => arr.lift(index)
          case _ => None
        }
      }
      
      // TODO merge with Evaluator impl
      private def unlift[A, B](f: A => Option[B]): PartialFunction[A, B] = new PartialFunction[A, B] {
        def apply(a: A) = f(a).get
        def isDefinedAt(a: A) = f(a).isDefined
      }
    }
    
    def fullProjection(userUID: String, path: Path, expiresAt: Long)(implicit asyncContext: ExecutionContext): Dataset[SValue] = 
      IterableDataset(1, new Iterable[(Identities, SValue)] { def iterator = readJSON(path) })
    
    def mask(userUID: String, path: Path): DatasetMask[Dataset] = StubDatasetMask(userUID, path, Vector())
    
    private def readJSON(path: Path): Iterator[SEvent] = {
      val src = Source.fromInputStream(getClass getResourceAsStream path.elements.mkString("/", "/", ".json"))
      val stream = Stream from 0 map scaleId(path) zip (src.getLines map parseJSON toStream) map tupled(wrapSEvent)
      //Iteratee.enumPStream[X, Vector[SEvent], IO](stream.grouped(chunkSize).map(Vector(_: _*)).toStream)
      stream.iterator
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
      (VectorCase(id), wrapSValue(value))
    
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
