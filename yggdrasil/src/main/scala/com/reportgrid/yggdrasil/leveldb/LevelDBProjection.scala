package com.reportgrid.yggdrasil
package leveldb

import com.reportgrid.analytics.Path
import com.reportgrid.common._ 
import com.reportgrid.util._ 
import com.reportgrid.util.Bijection._

import org.iq80.leveldb._
import org.fusesource.leveldbjni.JniDBFactory._
import java.io._
import java.nio.Buffer
import java.nio.ByteBuffer
import Bijection._

import com.weiglewilczek.slf4s.Logger
import scala.collection.JavaConverters._
import scala.collection.Iterator
import scala.io.Source

import scalaz.{Ordering => _, Source => _, _}
import scalaz.Scalaz._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.iteratee.Input._
import scalaz.syntax.plus._
import scalaz.syntax.monad._
import scalaz.syntax.applicativePlus._
import scalaz.syntax.biFunctor
import scalaz.Scalaz._
import IterateeT._

import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.JsonParser
import blueeyes.json.xschema._
import blueeyes.json.xschema.Extractor._
import blueeyes.json.xschema.DefaultSerialization._

object LevelDBProjectionComparator {
  def apply(_descriptor: ProjectionDescriptor) = new DBComparator {
    val projection: ByteProjection = new LevelDBByteProjection {
      val descriptor = _descriptor
    }

    def name = _descriptor.serialize
    def compare(k1: Array[Byte], k2: Array[Byte]) = projection.keyOrder.order(k1, k2).toInt

    // default implementations
    def findShortestSeparator(start: Array[Byte], limit: Array[Byte]) = start
    def findShortSuccessor(key: Array[Byte]) = key
  }
}

object LevelDBProjection {
  private final val comparatorMetadataFilename = "comparator"

  def apply(baseDir : File, descriptor: ProjectionDescriptor): ValidationNEL[Throwable, LevelDBProjection] = {
    val baseDirV = if (! baseDir.exists && ! baseDir.mkdirs()) (new RuntimeException("Could not create database basedir " + baseDir): Throwable).fail[File] 
                   else baseDir.success[Throwable]

    baseDirV.toValidationNel map { (bd: File) => new LevelDBProjection(bd, descriptor) }
  }

  val descriptorFile = "projection_descriptor.json"
  def descriptorSync(baseDir: File): Sync[ProjectionDescriptor] = new CommonSync[ProjectionDescriptor](baseDir, descriptorFile)

  private class CommonSync[T](baseDir: File, filename: String)(implicit extractor: Extractor[T], decomposer: Decomposer[T]) extends Sync[T] {
    import java.io._

    val df = new File(baseDir, filename)

    def read = 
      if (df.exists) Some {
        IO {
          val reader = new FileReader(df)
          try {
            { (err: Extractor.Error) => err.message } <-: JsonParser.parse(reader).validated(extractor)
          } finally {
            reader.close
          }
        }
      } else {
        None
      }

    def sync(data: T) = IO {
      Validation.fromTryCatch {
        val tmpFile = File.createTempFile(filename, ".tmp", baseDir)
        val writer = new FileWriter(tmpFile)
        try {
          compact(render(data.serialize(decomposer)), writer)
        } finally {
          writer.close
        }
        tmpFile.renameTo(df) // TODO: This is only atomic on POSIX systems
        Success(())
      }
    }
  }
}

class LevelDBProjection private (val baseDir: File, val descriptor: ProjectionDescriptor) extends LevelDBByteProjection with Projection {
  import LevelDBProjection._

  val logger = Logger("col:" + baseDir)
  logger.debug("Opening column index files")

  private val createOptions = (new Options).createIfMissing(true)
  private val idIndexFile: DB =  factory.open(new File(baseDir, "idIndex"), createOptions.comparator(LevelDBProjectionComparator(descriptor)))
  private lazy val valIndexFile: DB = {
    sys.error("Value-based indexes have not been enabled.")
     //factory.open(new File(baseDir, "valIndex"), createOptions.comparator(comparator))
  }

  private final val syncOptions = (new WriteOptions).sync(true)

  def close: IO[Unit] = IO {
    logger.info("Closing column index files")
    idIndexFile.close()
    //valIndexFile.close()
  }

  def sync: IO[Unit] = IO { } 

  def valueOffsets(values: Array[Byte]): List[Int] = {
    val buf = ByteBuffer.wrap(values)
    val positions = descriptor.columns.map(_.valueType.format).foldLeft(List(0)) {
      case (v :: vx, FixedWidth(w))  => (v + w) :: v :: vx
      case (v :: vx, LengthEncoded)  => (v + buf.getInt(v)) :: v :: vx
    } 

    positions.tail.reverse
  }

  def insert(id : Identities, v : Seq[CValue], shouldSync: Boolean = false): IO[Unit] = IO {
    val (idBytes, valueBytes) = project(id, v)

    if (shouldSync) {
      idIndexFile.put(idBytes, valueBytes, syncOptions)
    } else {
      idIndexFile.put(idBytes, valueBytes)
    }
  }

  ///////////////////
  // ID Traversals //
  ///////////////////

  def traverseIndex[X, E, F[_]](f: (Identities, Seq[CValue]) => E)(implicit MO: F |>=| IO): EnumeratorT[X, E, F] = {
    import MO._
    import MO.MG.bindSyntax._

    def enumerator(iter : DBIterator, close : IO[Unit]): EnumeratorT[X, E, F] = new EnumeratorT[X, E, F] { 
      def apply[A] = (s : StepT[X, E, F, A]) => {
        val _done = iterateeT[X, E, F, A](MO.promote(close) >> s.pointI.value)

        s.fold(
          cont = k => if (iter.hasNext) {
            val n = iter.next
            k(elInput((unproject(n.getKey, n.getValue)(f)))) >>== apply[A]
          } else {
            _done
          },
          done = (_, _) => _done,
          err = _ => _done
        )
      }
    }

    val iter = idIndexFile.iterator
    iter.seekToFirst()
    enumerator(iter, IO(iter.close))
  }

  def getAllPairs[X] : EnumeratorP[X, (Identities, Seq[CValue]), IO] = new EnumeratorP[X, (Identities, Seq[CValue]), IO] {
    def apply[F[_]](implicit MO: F |>=| IO) = traverseIndex[X, (Identities, Seq[CValue]), F] {
      (id, b) => (id, b)
    }
  }

  def getAllIds[X] : EnumeratorP[X, Identities, IO] = new EnumeratorP[X, Identities, IO] {
    def apply[F[_]](implicit MO: F |>=| IO) = traverseIndex[X, Identities, F] {
      (id, b) => id
    }
  }

  def getAllValues[X] = new EnumeratorP[X, Seq[CValue], IO] { 
    def apply[F[_]](implicit MO: F |>=| IO) = traverseIndex[X, Seq[CValue], F] {
      (id, b) => b
    }
  }

  def getColumnValues[X](col: ColumnDescriptor): EnumeratorP[X, (Identities, CValue), IO] = new EnumeratorP[X, (Identities, CValue), IO] {
    val columnIndex = descriptor.columns.indexOf(col)
    def apply[F[_]](implicit MO: F |>=| IO) = traverseIndex[X, (Identities, CValue), F] {
      (id, b) => (id, b(columnIndex))
    }
  }

  def traverseIndexRange[X, E, F[_]](range: Interval[Identities])(f: (Identities, Seq[CValue]) => E)(implicit MO : F |>=| IO): EnumeratorT[X, E, F] = {
    import MO._
    import MO.MG.bindSyntax._

    def enumerator(iter: DBIterator, close: IO[Unit]): EnumeratorT[X, E, F] = new EnumeratorT[X, E, F] { 
      def apply[A] = (s: StepT[X, E, F, A]) => {
        @inline def _done = iterateeT[X, E, F, A](MO.promote(close) >> s.pointI.value)

        s.fold(
          cont = k => if (iter.hasNext) {
            val n = iter.next
            val id = n.getKey.as[Identities]
            range.end match {
              case Some(end) if end <= id => _done
              case _ => k(elInput(unproject(n.getKey, n.getValue)(f))) >>== apply[A]
            }
          } else _done,
          done = (_, _) => _done,
          err = _ => _done
        )
      }
    }
  
    val iter = idIndexFile.iterator
    range.start match {
      case Some(id) => iter.seek(id.as[Array[Byte]])
      case None => iter.seekToFirst()
    }

    enumerator(iter, IO(iter.close))
  }

  def getPairsByIdRange[X](range: Interval[Identities]): EnumeratorP[X, (Identities, Seq[CValue]), IO] = new EnumeratorP[X, (Identities, Seq[CValue]), IO] {
    def apply[F[_]](implicit MO: F |>=| IO) = traverseIndexRange[X, (Identities, Seq[CValue]), F](range) {
      (id, b) => (id, b)
    }
  }

  def getPairForId[X](id: Identities): EnumeratorP[X, (Identities, Seq[CValue]), IO] = 
    getPairsByIdRange(Interval(Some(id), Some(id)))


  /**
   * Retrieve all IDs for IDs in the given range [start,end]
   */  
  def getIdsInRange[X](range : Interval[Identities]) = new EnumeratorP[X, Identities, IO] {
    def apply[F[_]](implicit MO: F |>=| IO) = traverseIndexRange[X, Identities, F](range) {
      (id, b) => id
    }
  }

  /**
   * Retrieve all values for IDs in the given range [start,end]
   */  
  def getValuesByIdRange[X](range: Interval[Identities]) = new EnumeratorP[X, Seq[CValue], IO] {
    def apply[F[_]](implicit MO: F |>=| IO) = traverseIndexRange[X, Seq[CValue], F](range) {
      (id, b) => b
    }
  }

  def getValueForId[X](id: Identities): EnumeratorP[X, Seq[CValue], IO] = 
    getValuesByIdRange(Interval(Some(id), Some(id)))

  //////////////////////
  // Value Traversals //
  //////////////////////

//  /**
//   * Retrieve all distinct values.
//   */
//  def getAllValues[F[_] : MonadIO, A] : EnumeratorT[Unit, Array[Byte], F, A] = {
//    def valuePart(arr: Array[Byte]) = arr.take(arr.length - 8)
//    def enumerator(iter : DBIterator, close : F[Unit]): EnumeratorT[Unit, Array[Byte], F, A] = { s =>
//      s.fold(
//        cont = k => if (iter.hasNext) {
//          logger.trace("Processing next valIndex value")
//          val valueKey = iter.next.getKey
//          val value = valuePart(valueKey)
//
//          // advance the iterator until the next value to be taken from it is either the end of the iterator
//          // or not equal to the current value (this operation gives 'distinct' semantics)
//          while (iter.hasNext && comparator.compare(valueKey, iter.peekNext.getKey) == 0) {
//            logger.trace("  advancing iterator on same value")
//            iter.next
//          }
//
//          k(elInput(value)) >>== enumerator(iter, close)
//        } else {
//          logger.trace("No more values")
//          iterateeT(close >> s.pointI.value)
//        }, 
//        done = (_, _) => { logger.trace("Done with iteration"); iterateeT(close >> s.pointI.value) },
//        err = _ => { logger.trace("Error on iteration"); iterateeT(close >> s.pointI.value) }
//      )
//    }
//       
//    val iter = valIndexFile.iterator 
//    iter.seekToFirst()
//    enumerator(iter, IO(iter.close).liftIO[F])
//  }
//
//  /**
//   * Retrieve all IDs for values in the given range [start,end]
//   */
//  def getIdsByValueRange[F[_] : MonadIO, A](range : Interval[Array[Byte]]): EnumeratorT[Unit, Identities, F, A] = {
//    def enumerator(iter: DBIterator, close: F[Unit]): EnumeratorT[Unit, Identities, F, A] = { s =>
//      s.fold(
//        cont = k => if (iter.hasNext) {
//          val n = iter.next
//          val (cv, ci) = n.getKey.splitAt(n.getKey.length - 8)
//          range.end match {
//            case Some(end) if comparator.compare(cv, end) >= 0 => iterateeT(close >> s.pointI.value)
//            case _ => k(elInput(ci.as[Identities])) >>== enumerator(iter, close)
//          }
//        } else {
//          iterateeT(close >> s.pointI.value)
//        },
//        done = (_, _) => iterateeT(close >> s.pointI.value),
//        err = _ => iterateeT(close >> s.pointI.value)
//      )
//    }
//
//    val iter = valIndexFile.iterator
//    range.start match {
//      case Some(v) => iter.seek(columnKeys(0L, v)._2)
//      case None    => iter.seekToFirst()
//    }
//
//    enumerator(iter, IO(iter.close).liftIO[F])
//  }
//
//  /**
//   * Retrieve all IDs for the given value
//   */
//  def getIdsForValue[F[_] : MonadIO, A](v : Array[Byte]): EnumeratorT[Unit, Identities, F, A] = 
//    getIdsByValueRange(Interval(Some(v), Some(v)))
//
//  /**
//   * Retrieve all values in the given range [start,end]
//   */
//  def getValuesInRange[F[_] : MonadIO, A](range : Interval[Array[Byte]]): EnumeratorT[Unit, Array[Byte], F, A] = {
//    def enumerator(iter: DBIterator, close: F[Unit]): EnumeratorT[Unit, Array[Byte], F, A] = { s =>
//      s.fold(
//        cont = k => if (iter.hasNext) {
//          val n = iter.next
//          val (cv,ci) = n.getKey.splitAt(n.getKey.length - 8)
//          range.end match {
//            case Some(end) if comparator.compare(cv, end) >= 0 => iterateeT(close >> s.pointI.value)
//            case _ => k(elInput(cv)) >>== enumerator(iter, close)
//          }
//        } else {
//          iterateeT(close >> s.pointI.value)
//        },
//        done = (_, _) => iterateeT(close >> s.pointI.value),
//        err = _ => iterateeT(close >> s.pointI.value)
//      )
//    }
//
//    val iter = valIndexFile.iterator
//    range.start match {
//      case Some(v) => 
//        val (_, valIndexBytes) = columnKeys(0L, v)
//        iter.seek(valIndexBytes)
//      case None => iter.seekToFirst()
//    }
//    enumerator(iter, IO(iter.close).liftIO[F])
//  }
}
