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

case class DatasetEnum[X, E, F[_]](enum: EnumeratorP[X, E, F], descriptor: Option[ProjectionDescriptor] = None) { //, identityDerivation: Vector[IdentitySource]) 
  def map[E2](f: E => E2): DatasetEnum[X, E2, F] = DatasetEnum(
    new EnumeratorP[X, E2, F] {
      def apply[G[_]](implicit MO: G |>=| F): EnumeratorT[X, E2, G] = new EnumeratorT[X, E2, G] {
        import MO._
        def apply[A] = enum[G].map(f).apply[A]
      }
    }
  )

  def flatMap[E2](f: E => DatasetEnum[X, E2, F]): DatasetEnum[X, E2, F] = DatasetEnum(
    new EnumeratorP[X, E2, F] {
      def apply[G[_]](implicit MO: G |>=| F): EnumeratorT[X, E2, G] = {
        import MO._
        enum[G].flatMap(e => f(e).enum[G])
      }
    }
  )
  
  def collect[E2](pf: PartialFunction[E, E2]): DatasetEnum[X, E2, F] = DatasetEnum(
    new EnumeratorP[X, E2, F] {
      def apply[G[_]](implicit MO: G |>=| F): EnumeratorT[X, E2, G] = {
        import MO._
        enum[G].collect(pf)
      }
    }
  )

  def uniq(implicit ord: Order[E]): DatasetEnum[X, E, F] = DatasetEnum(
    new EnumeratorP[X, E, F] {
      def apply[G[_]](implicit MO: G |>=| F): EnumeratorT[X, E, G] = {
        import MO._
        enum[G].uniq
      }
    }
  )

  def zipWithIndex: DatasetEnum[X, (E, Long), F] = DatasetEnum(
    new EnumeratorP[X, (E, Long), F] {
      def apply[G[_]](implicit MO: G |>=| F): EnumeratorT[X, (E, Long), G] = {
        import MO._
        enum[G].zipWithIndex
      }
    }
  )

  def :*[E2](enum2: DatasetEnum[X, E2, F]): DatasetEnum[X, (E, E2), F] = DatasetEnum(
    new EnumeratorP[X, (E, E2), F] {
      def apply[G[_]](implicit MO: G |>=| F) = {
        import MO._
        cross(enum[G], enum2.enum[G])
      }
    }
  )

  def *:[E2](enum2: DatasetEnum[X, E2, F]): DatasetEnum[X, (E2, E), F] = DatasetEnum(
    new EnumeratorP[X, (E2, E), F] {
      def apply[G[_]](implicit MO: G |>=| F) = {
        import MO._
        cross(enum2.enum[G], enum[G])
      }
    }
  )

  def join(enum2: DatasetEnum[X, E, F])(implicit order: Order[E], m: Monad[F]): DatasetEnum[X, (E, E), F] =
    DatasetEnum(matchE[X, E, F].apply(enum, enum2.enum))

  def merge(enum2: DatasetEnum[X, E, F])(implicit order: Order[E], monad: Monad[F]): DatasetEnum[X, E, F] =
    DatasetEnum(mergeE[X, E, F].apply(enum, enum2.enum))
}

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
    enum1 :* enum2

  def crossRight[X, F[_]: Monad](enum1: DatasetEnum[X, SEvent, F], enum2: DatasetEnum[X, SEvent, F]): DatasetEnum[X, (SEvent, SEvent), F] = 
    enum1 *: enum2

  def join[X, F[_]](enum1: DatasetEnum[X, SEvent, F], enum2: DatasetEnum[X, SEvent, F])(implicit order: Order[SEvent], monad: Monad[F]): DatasetEnum[X, (SEvent, SEvent), F] =
    enum1 join enum2

  def merge[X, F[_]](enum1: DatasetEnum[X, SEvent, F], enum2: DatasetEnum[X, SEvent, F])(implicit order: Order[SEvent], monad: Monad[F]): DatasetEnum[X, SEvent, F] =
    enum1 merge enum2

  def sort[X](enum: DatasetEnum[X, SEvent, IO])(implicit order: Order[SEvent]): DatasetEnum[X, SEvent, IO] 
  
  def empty[X, E, F[_]: Monad]: DatasetEnum[X, E, F] = DatasetEnum(
    new EnumeratorP[X, E, F] {
      def apply[G[_]](implicit MO: G |>=| F): EnumeratorT[X, E, G] = {
        import MO._
        Monoid[EnumeratorT[X, E, G]].zero
      }
    }
  )

  def point[X, E, F[_]: Monad](value: E): DatasetEnum[X, E, F] = DatasetEnum(
    new EnumeratorP[X, E, F] {
      def apply[G[_]](implicit MO: G |>=| F): EnumeratorT[X, E, G] = {
        import MO._
        EnumeratorT.enumOne[X, E, G](value)
      }
    }
  )

  def liftM[X, E, F[_]: Monad](value: F[E]): DatasetEnum[X, E, F] = DatasetEnum(
    new EnumeratorP[X, E, F] {
      def apply[G[_]](implicit MO: G |>=| F): EnumeratorT[X, E, G] = new EnumeratorT[X, E, G] {
        import MO._
        import MO.MG.bindSyntax._

        def apply[A] = { (step: StepT[X, E, G, A]) => 
          iterateeT[X, E, G, A](MO.promote(value) >>= { e => step.mapCont(f => f(elInput(e))).value })
        }
      }
    }
  )

  def map[X, E1, E2, F[_]: Monad](enum: DatasetEnum[X, E1, F])(f: E1 => E2): DatasetEnum[X, E2, F] = 
    enum.map(f)

  def flatMap[X, E1, E2, F[_]: Monad](enum: DatasetEnum[X, E1, F])(f: E1 => DatasetEnum[X, E2, F]): DatasetEnum[X, E2, F] = 
    enum.flatMap(f)
     
  def collect[X, E1, E2, F[_]: Monad](enum: DatasetEnum[X, E1, F])(pf: PartialFunction[E1, E2]): DatasetEnum[X, E2, F] = 
    enum.collect(pf)
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
    ((consume[Unit, SEvent, IO, Set] &= eval(graph).enum[IO]) run { err => sys.error("O NOES!!!") }) unsafePerformIO
}
