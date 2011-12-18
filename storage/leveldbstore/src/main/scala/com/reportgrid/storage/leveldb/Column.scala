package com.reportgrid.storage.leveldb

import org.iq80.leveldb._
import org.fusesource.leveldbjni.JniDBFactory._
import java.io._
import java.math.BigDecimal
import java.nio.ByteBuffer
import Bijection._

import com.weiglewilczek.slf4s.Logger
import scalaz.Scalaz._
import scala.collection.JavaConverters._
import scala.collection.Iterator
import scala.io.Source

import scalaz.{Ordering => _, Source => _, _}
import scalaz.effect._
import scalaz.iteratee._
import scalaz.iteratee.Input._
import scalaz.syntax.plus._
import scalaz.syntax.applicativePlus._
import scalaz.Scalaz._
import IterateeT._
//import scalaz.Scalaz._

object Column {
  private final val comparatorMetadataFilename = "comparator"

  def restoreComparator(baseDir: File) : Validation[Throwable, DBComparator] = {
    val comparatorMetadata = new File(baseDir, comparatorMetadataFilename)

    for {
      source          <- Validation.fromTryCatch(Source.fromFile(comparatorMetadata, "UTF-8"))
      line            <- source.getLines.toList.headOption.toSuccess(new RuntimeException("Comparator metadata file was empty."): Throwable)
      comparatorClass <- Validation.fromTryCatch(Class.forName(line.trim).asInstanceOf[Class[DBComparator]])
      comparator      <- Validation.fromTryCatch(comparatorClass.newInstance)
    } yield {
      comparator
    }
  }

  def saveComparator(baseDir: File, comparator: DBComparator) : Validation[Throwable, DBComparator] = {
    Validation.fromTryCatch {
      import java.io.{FileOutputStream,OutputStreamWriter}
      val output = new OutputStreamWriter(new FileOutputStream(new File(baseDir, comparatorMetadataFilename)), "UTF-8")
      try {
        output.write(comparator.getClass.getName)
      } finally {
        output.close()
      }
      comparator
    }
  }

  def columnKeys(id: Long, v: ByteBuffer) = {
    val idBytes = id.as[Array[Byte]]
    val vBytes = v.as[Array[Byte]]

    (idBytes, ByteBuffer.allocate(idBytes.length + vBytes.length).put(vBytes).put(idBytes).array)
  }

  def apply(baseDir : File, comparator: Option[DBComparator] = None) = {
    val baseDirV = if (! baseDir.exists && ! baseDir.mkdirs()) (new RuntimeException("Could not create database basedir " + baseDir): Throwable).fail[File] 
                   else baseDir.success[Throwable]

    val comparatorV = for {
      bd <- baseDirV.toValidationNel
      c <- restoreComparator(bd).toValidationNel orElse comparator.toSuccess(new RuntimeException("No database comparator was provided."): Throwable).flatMap(saveComparator(bd, _)).toValidationNel
    } yield c

    (baseDirV.toValidationNel |@| comparatorV) { (bd, c) => new Column(bd, c) }
  }
}

class Column(baseDir : File, comparator: DBComparator) {
  import Column._

  val logger = Logger("col:" + baseDir)
  logger.debug("Opening column index files")

  private val createOptions = (new Options).createIfMissing(true)
  private val idIndexFile =  factory.open(new File(baseDir, "idIndex"), createOptions)
  private val valIndexFile = factory.open(new File(baseDir, "valIndex"), createOptions.comparator(comparator))

  private final val syncOptions = (new WriteOptions).sync(true)

  def close: IO[Unit] = IO {
    logger.info("Closing column index files")
    idIndexFile.close()
    valIndexFile.close()
  }

  def insert(id : Long, v : ByteBuffer, shouldSync: Boolean = false): IO[Unit] = IO {
    val (idBytes, valIndexBytes) = columnKeys(id, v)

    if (shouldSync) {
      valIndexFile.put(valIndexBytes, Array[Byte](), syncOptions)
      idIndexFile.put(idBytes, v, syncOptions)
    } else {
      valIndexFile.put(valIndexBytes, Array[Byte]())
      idIndexFile.put(idBytes, v)
    }
  }

  /**
   * Retrieve all values for IDs in the given range [start,end]
   */  
  def getValuesByIdRange[F[_]: MonadIO, A](range: Interval[Long]): EnumeratorT[Unit, ByteBuffer, F, A] = {
    def enumerator(iter: DBIterator, close: F[Unit]): EnumeratorT[Unit, ByteBuffer, F, A] = { s => 
      s.fold(
        cont = k => if (iter.hasNext) {
          val n = iter.next
          range.end match {
            case Some(end) if end <= n.getKey.as[Long] => iterateeT(close >> s.pointI.value)
            case _ => k(elInput(ByteBuffer.wrap(n.getValue))) >>== enumerator(iter, close)
          }
        } else {
          iterateeT(close >> s.pointI.value)
        },
        done = (_, _) => iterateeT(close >> s.pointI.value),
        err = _ => iterateeT(close >> s.pointI.value)
      )
    }
    
    val iter = idIndexFile.iterator
    range.start match {
      case Some(id) => iter.seek(id.as[Array[Byte]])
      case None => iter.seekToFirst()
    }

    enumerator(iter, IO(iter.close).liftIO[F])
  }

  /**
   * Retrieve all IDs for the given value
   */
  def getIdsForValue[F[_] : MonadIO, A](v : ByteBuffer): EnumeratorT[Unit, ByteBuffer, F, A] = {
    val vBytes = v.as[Array[Byte]]
    def enumerator(iter: DBIterator, close: F[Unit]): EnumeratorT[Unit, ByteBuffer, F, A] = { s =>
      s.fold(
        cont = k => if (iter.hasNext) {
          val n = iter.next
          val (cv,ci) = n.getKey.splitAt(n.getKey.length - 8)
          if (comparator.compare(cv, vBytes) > 0) iterateeT(close >> s.pointI.value)
          else k(elInput(ByteBuffer.wrap(ci))) >>== enumerator(iter, close)
        } else {
          iterateeT(close >> s.pointI.value)
        },
        done = (_, _) => iterateeT(close >> s.pointI.value),
        err = _ => iterateeT(close >> s.pointI.value)
      )
    }

    val iter = valIndexFile.iterator
    val (_, valIndexBytes) = columnKeys(0L, v)
    iter.seek(valIndexBytes)
    enumerator(iter, IO(iter.close).liftIO[F])
  }

  /**
   * Retrieve all IDs for values in the given range [start,end]
   */
  def getIdsByValueRange[F[_] : MonadIO, A](range : Interval[ByteBuffer]): EnumeratorT[Unit, ByteBuffer, F, A] = {
    val rangeEnd = range.end.map(_.as[Array[Byte]])
    def enumerator(iter: DBIterator, close: F[Unit]): EnumeratorT[Unit, ByteBuffer, F, A] = { s =>
      s.fold(
        cont = k => if (iter.hasNext) {
          val n = iter.next
          val (cv,ci) = n.getKey.splitAt(n.getKey.length - 8)
          rangeEnd match {
            case Some(end) if comparator.compare(cv, end) >= 0 => iterateeT(close >> s.pointI.value)
            case _ => k(elInput(ByteBuffer.wrap(ci))) >>== enumerator(iter, close)
          }
        } else {
          iterateeT(close >> s.pointI.value)
        },
        done = (_, _) => iterateeT(close >> s.pointI.value),
        err = _ => iterateeT(close >> s.pointI.value)
      )
    }

    val iter = valIndexFile.iterator
    range.start match {
      case Some(v) => 
        val (_, valIndexBytes) = columnKeys(0L, v)
        iter.seek(valIndexBytes)
      case None => iter.seekToFirst()
    }
    enumerator(iter, IO(iter.close).liftIO[F])
  }

  def getAllIds[F[_] : MonadIO, A] : EnumeratorT[Unit, ByteBuffer, F, A] = {
    def enumerator(iter : DBIterator, close : F[Unit]): EnumeratorT[Unit, ByteBuffer, F, A] = { s =>
      s.fold(
        cont = k => if (iter.hasNext) {
          k(elInput(ByteBuffer.wrap(iter.next.getKey))) >>== enumerator(iter, close)
        } else {
          iterateeT(close >> s.pointI.value)
        }, 
        done = (_, _) => iterateeT(close >> s.pointI.value),
        err = _ => iterateeT(close >> s.pointI.value)
      )
    }
       
    val iter = idIndexFile.iterator 
    iter.seekToFirst()
    enumerator(iter, IO(iter.close).liftIO[F])
  }

  def getAllValues[F[_] : MonadIO, A] : EnumeratorT[Unit, ByteBuffer, F, A] = {
    def valuePart(arr: Array[Byte]) = arr.take(arr.length - 8)
    def enumerator(iter : DBIterator, close : F[Unit]): EnumeratorT[Unit, ByteBuffer, F, A] = { s =>
      s.fold(
        cont = k => if (iter.hasNext) {
          logger.trace("Processing next valIndex value")
          val valueKey = iter.next.getKey
          val value = valuePart(valueKey)

          // advance the iterator until the next value to be taken from it is either the end of the iterator
          // or not equal to the current value (this operation gives 'distinct' semantics)
          while (iter.hasNext && comparator.compare(valueKey, iter.peekNext.getKey) == 0) {
            logger.trace("  advancing iterator on same value")
            iter.next
          }

          k(elInput(ByteBuffer.wrap(value))) >>== enumerator(iter, close)
        } else {
          logger.trace("No more values")
          iterateeT(close >> s.pointI.value)
        }, 
        done = (_, _) => { logger.trace("Done with iteration"); iterateeT(close >> s.pointI.value) },
        err = _ => { logger.trace("Error on iteration"); iterateeT(close >> s.pointI.value) }
      )
    }
       
    val iter = valIndexFile.iterator 
    iter.seekToFirst()
    enumerator(iter, IO(iter.close).liftIO[F])
  }
}
