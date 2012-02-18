package com.precog.daze

import com.precog.yggdrasil._
import com.precog.analytics.Path

import akka.dispatch.Await
import akka.dispatch.ExecutionContext
import akka.dispatch.Future
import akka.util.duration._

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
  implicit val asyncContext: akka.dispatch.ExecutionContext = ExecutionContext.defaultExecutionContext(actorSystem)
}

// TODO decouple this from the evaluator specifics
trait DatasetConsumers extends Evaluator {
  def maxEvalDuration: akka.util.Duration

  def consumeEval(graph: DepGraph): Set[SEvent] = {
    val results = Await.result(
      eval(graph).fenum.map { (enum: EnumeratorP[Unit, Vector[SEvent], IO]) => 
        try {
          Right(((consume[Unit, Vector[SEvent], IO, Set] &= enum[IO]) run { err => sys.error("O NOES!!!") }) unsafePerformIO)
        } catch {
          case e => Left(e)
        }
      },
      maxEvalDuration
    )

    results.left foreach { throw _ }

    val Right(back) = results
    back.flatten
  }
}

trait StubOperationsAPI 
    extends StorageEngineQueryComponent
    with YggdrasilEnumOpsComponent
    with DatasetConsumers { self =>

  implicit def asyncContext = StubOperationsAPI.asyncContext
  
  object query extends QueryAPI {
    val chunkSize = 2000
  }
  
  trait QueryAPI extends StorageEngineQueryAPI {
    private var pathIds = Map[Path, Int]()
    private var currentId = 0

    def chunkSize: Int
    
    private case class StubDatasetMask[X](path: Path, selector: Vector[Either[Int, String]]) extends DatasetMask[X] {
      def derefObject(field: String): DatasetMask[X] = copy(selector = selector :+ Right(field))
      def derefArray(index: Int): DatasetMask[X] = copy(selector = selector :+ Left(index))
      def typed(tpe: SType): DatasetMask[X] = this
      
      def realize(implicit asyncContext: akka.dispatch.ExecutionContext): DatasetEnum[X, SEvent, IO] = {
        fullProjection[X](path) collect unlift(mask)
      }
      
      private def mask(sev: SEvent): Option[SEvent] = {
        val (ids, sv) = sev
        
        val result = selector.foldLeft(Some(sv): Option[SValue]) {
          case (None, _) => None
          case (Some(SObject(obj)), Right(field)) => obj get field
          case (Some(SArray(arr)), Left(index)) => arr.lift(index)
          case _ => None
        }
        
        result map { sv => (ids, sv) }
      }
      
      // TODO merge with Evaluator impl
      private def unlift[A, B](f: A => Option[B]): PartialFunction[A, B] = new PartialFunction[A, B] {
        def apply(a: A) = f(a).get
        def isDefinedAt(a: A) = f(a).isDefined
      }
    }
    
    def fullProjection[X](path: Path)(implicit asyncContext: ExecutionContext): DatasetEnum[X, SEvent, IO] =
      DatasetEnum(akka.dispatch.Promise.successful(readJSON[X](path)))
    
    def mask[X](path: Path): DatasetMask[X] = StubDatasetMask(path, Vector())
    
    private def readJSON[X](path: Path) = {
      val src = Source.fromInputStream(getClass getResourceAsStream path.elements.mkString("/", "/", ".json"))
      val stream = Stream from 0 map scaleId(path) zip (src.getLines map parseJSON toStream) map tupled(wrapSEvent)
      Iteratee.enumPStream[X, Vector[SEvent], IO](stream.grouped(chunkSize).map(Vector(_: _*)).toStream)
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
