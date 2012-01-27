package com.querio
package daze

import com.reportgrid.yggdrasil._
import com.reportgrid.yggdrasil.util.Enumerators
import com.reportgrid.analytics.Path
import java.io.File
import scalaz.{Identity => _, _}
import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.set._

import Iteratee._

// TODO this file desparately needs to be split up

//case class IdentitySource(sources: Set[ProjectionDescriptor])
case class EventMatcher(order: Order[SEvent], merge: (Vector[Identity], Vector[Identity]) => Vector[Identity])

case class DatasetEnum[X, E, F[_]](enum: EnumeratorP[X, E, F], descriptor: Option[ProjectionDescriptor] = None) //, identityDerivation: Vector[IdentitySource]) 

// QualifiedSelector(path: String, sel: JPath, valueType: EType)
trait StorageEngineInsertAPI

trait StorageEngineQueryAPI {
  def fullProjection[X](path: Path): DatasetEnum[X, SEvent, IO]

  //def column(path: String, selector: JPath, valueType: EType): DatasetEnum[X, SEvent, IO]
  //def columnRange(interval: Interval[ByteBuffer])(path: String, selector: JPath, valueType: EType): DatasetEnum[X, (Seq[Long], ByteBuffer), IO]
}

trait StorageEngineAPI extends StorageEngineInsertAPI with StorageEngineQueryAPI 

trait DatasetEnumFunctions {
  def cogroup[X, F[_]](enum1: DatasetEnum[X, SEvent, F], enum2: DatasetEnum[X, SEvent, F])(implicit order: Order[SEvent], monad: Monad[F]): DatasetEnum[X, Either3[SEvent, (SEvent, SEvent), SEvent], F] = 
    DatasetEnum(cogroupE[X, SEvent, F].apply(enum1.enum, enum2.enum))

  def crossLeft[X, F[_]: Monad](enum1: DatasetEnum[X, SEvent, F], enum2: DatasetEnum[X, SEvent, F]): DatasetEnum[X, (SEvent, SEvent), F] =
    DatasetEnum(crossE(enum1.enum, enum2.enum))

  def crossRight[X, F[_]: Monad](enum1: DatasetEnum[X, SEvent, F], enum2: DatasetEnum[X, SEvent, F]): DatasetEnum[X, (SEvent, SEvent), F] =
    DatasetEnum(crossE(enum2.enum, enum1.enum))

  def join[X, F[_]](enum1: DatasetEnum[X, SEvent, F], enum2: DatasetEnum[X, SEvent, F])(implicit order: Order[SEvent], monad: Monad[F]): DatasetEnum[X, (SEvent, SEvent), F] =
    DatasetEnum(matchE[X, SEvent, F].apply(enum1.enum, enum2.enum))

  def merge[X, F[_]](enum1: DatasetEnum[X, SEvent, F], enum2: DatasetEnum[X, SEvent, F])(implicit order: Order[SEvent], monad: Monad[F]): DatasetEnum[X, SEvent, F] =
    DatasetEnum(mergeE[X, SEvent, F].apply(enum1.enum, enum2.enum))

  def sort[X](enum: DatasetEnum[X, SEvent, IO])(implicit order: Order[SEvent]): DatasetEnum[X, SEvent, IO] 
  
  def map[X, E1, E2, G[_]: Monad](enum: DatasetEnum[X, E1, G])(f: E1 => E2): DatasetEnum[X, E2, G] = DatasetEnum(
    new EnumeratorP[X, E2, G] {
      def apply[F[_[_], _]: MonadTrans]: EnumeratorT[X, E2, ({ type λ[α] = F[G, α] })#λ] = new EnumeratorT[X, E2, ({ type λ[α] = F[G, α] })#λ] {
        def apply[A] = enum.enum[F].map(f)(MonadTrans[F].apply[G]).apply[A]
      }
    }
  )

  def empty[X, E, G[_]: Monad]: DatasetEnum[X, E, G] = DatasetEnum(
    new EnumeratorP[X, E, G] {
      def apply[F[_[_], _]: MonadTrans]: EnumeratorT[X, E, ({ type λ[α] = F[G, α] })#λ] = {
        type FG[a] = F[G, a]
        implicit val FMonad = MonadTrans[F].apply[G]
        Monoid[EnumeratorT[X, E, FG]].zero
      }
    }
  )

  def point[X, E, G[_]: Monad](value: E): DatasetEnum[X, E, G] = DatasetEnum(
    new EnumeratorP[X, E, G] {
      def apply[F[_[_], _]: MonadTrans] = EnumeratorT.enumOne[X, E, ({ type λ[α] = F[G, α] })#λ](value)(MonadTrans[F].apply[G])
    }
  )

  def liftM[X, E, G[_]: Monad](value: G[E]): DatasetEnum[X, E, G] = DatasetEnum(
    new EnumeratorP[X, E, G] {
      def apply[F[_[_], _]: MonadTrans]: EnumeratorT[X, E, ({ type λ[α] = F[G, α] })#λ] = new EnumeratorT[X, E, ({ type λ[α] = F[G, α] })#λ] {
        type FG[a] = F[G, a]
        implicit val FMonad = MonadTrans[F].apply[G]

        def apply[A] = { (step: StepT[X, E, FG, A]) => 
          iterateeT[X, E, FG, A](FMonad.bind(MonadTrans[F].liftM(value)) { e => step.mapCont(f => f(elInput(e))).value })
        }
      }
    }
  )

  def flatMap[X, E1, E2, G[_]: Monad](enum: DatasetEnum[X, E1, G])(f: E1 => DatasetEnum[X, E2, G]): DatasetEnum[X, E2, G] = DatasetEnum(
    new EnumeratorP[X, E2, G] {
      def apply[F[_[_], _]: MonadTrans]: EnumeratorT[X, E2, ({ type λ[α] = F[G, α] })#λ] = {
        implicit val FMonad = MonadTrans[F].apply[G]
        enum.enum[F].flatMap(e => f(e).enum[F])
      }
    }
  )
  
  def collect[X, E1, E2, G[_]: Monad](enum: DatasetEnum[X, E1, G])(pf: PartialFunction[E1, E2]): DatasetEnum[X, E2, G] = DatasetEnum(
    new EnumeratorP[X, E2, G] {
      def apply[F[_[_], _]: MonadTrans]: EnumeratorT[X, E2, ({ type λ[α] = F[G, α] })#λ] = {
        implicit val FMonad = MonadTrans[F].apply[G]
        enum.enum[F].collect(pf)
      }
    }
  )
}

trait OperationsAPI {
  def query: StorageEngineQueryAPI
  def ops: DatasetEnumFunctions
}

trait YggConfig {
  def workDir: File
  def sortBufferSize: Int
}

trait YggdrasilOperationsAPI extends OperationsAPI {
  def config: YggConfig

  def ops = new DatasetEnumFunctions {
    def sort[X](enum: DatasetEnum[X, SEvent, IO])(implicit order: Order[SEvent]): DatasetEnum[X, SEvent, IO] = {
      DatasetEnum(Enumerators.sort[X](enum.enum, config.sortBufferSize, config.workDir, enum.descriptor))
    }
  }
}

trait DefaultYggConfig {
  def config = new YggConfig {
    def workDir = {
      val tempFile = File.createTempFile("leveldb_tmp", "workdir")
      tempFile.delete //todo: validated
      tempFile.mkdir //todo: validated
      tempFile
    }

    def sortBufferSize = 10000
  }
}

// TODO decouple this from the evaluator specifics
trait StubQueryAPI extends OperationsAPI with Evaluator {
  import blueeyes.json._
  import scala.io.Source
  
  import JsonAST._
  import Function._
  import IterateeT._
  
  override object query extends StorageEngineQueryAPI {
    private var pathIds = Map[Path, Int]()
    private var currentId = 0
    
    def fullProjection[X](path: Path): DatasetEnum[X, SEvent, IO] =
      DatasetEnum(readJSON[X](path))
    
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
  
  protected def consumeEval(graph: DepGraph): Set[SEvent] = 
    (((consume[Unit, SEvent, ({ type λ[α] = IdT[IO, α] })#λ, Set] &= eval(graph).enum[IdT]) run { err => sys.error("O NOES!!!") }) run) unsafePerformIO
}
