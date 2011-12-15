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

import scalaz.{Ordering => _, _}
import scalaz.effect._
import scalaz.iteratee._
import scalaz.iteratee.Input._
import IterateeT._
import scalaz.Scalaz._

class Column[T](name : String, dataDir : String, order : Ordering[T])(implicit b : Bijection[T,Array[Byte]], comparator : Option[ColumnComparator[T]] = None) {
  val logger = Logger("col:" + name)

  private lazy val baseDir = {
    val bd = new File(dataDir,name)
    if (! bd.exists && ! bd.mkdirs()) {
      throw new IllegalStateException("Could not create the base directory: " + bd.getCanonicalPath)
    }
    bd
  }

  logger.debug("Opening column index files")

  private val createOptions = (new Options).createIfMissing(true)
  private val idIndexFile =  factory.open(new File(baseDir, "idIndex"), createOptions)
  comparator.foreach{ c => createOptions.comparator(c); logger.debug("Using custom comparator: " + c.name) }
  private val valIndexFile = factory.open(new File(baseDir, "valIndex"), createOptions)

  def sync() {
  }

  def close() {
    idIndexFile.close()
    valIndexFile.close()
  }

  private def shouldSync : Boolean = {
    false
  }

  def insert(id : Long, v : T) = {
    val idBytes = id.as[Array[Byte]]
    val valBytes = v.as[Array[Byte]]

    idIndexFile.put(idBytes, valBytes) 
    valIndexFile.put(valBytes ++ idBytes, Array[Byte]())
  }

  
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
      case None => iter.seekToFirst
    }

    enumerator(iter, IO(iter.close).liftIO[F])
  }

  def getIds[F[_] : MonadIO, A](v : T): EnumeratorT[Unit, Long, F, A] = {
    def enumerator(iter: DBIterator, close: F[Unit]): EnumeratorT[Unit, Long, F, A] = { s =>
      s.fold(
        cont = k => if (iter.hasNext) {
          val n = iter.next
          val (cv,ci) = n.getKey.splitAt(n.getKey.length - 8)
          if (order.compare(cv.as[T], v) > 0) iterateeT(close >> s.pointI.value)
          else k(elInput(ci.as[Long])) >>== enumerator(iter, close)
        } else {
          iterateeT(close >> s.pointI.value)
        },
        done = (_, _) => iterateeT(close >> s.pointI.value),
        err = _ => iterateeT(close >> s.pointI.value)
      )
    }

    val iter = valIndexFile.iterator
    iter.seek(v.as[Array[Byte]] ++ 0L.as[Array[Byte]])
    enumerator(iter, IO(iter.close).liftIO[F])
  }

/*
  def getIdsByValueRange(range : Interval[T])(implicit o : Ordering[T]): Stream[(T,Long)] = {
    import scala.math.Ordered._

    eval(valIndexFile) { db =>
      val iter = db.iterator

      range.start match {
        case Some(v) => iter.seek(v.as[Array[Byte]] ++ 0L.as[Array[Byte]])
        case None => iter.seekToFirst
      }

      val endCondition = range.end match {
        case Some(v) => (t : T) => t < v
        case None => (t : T) => true
      }

      iter.asScala.map(_.getKey).map{ kv => (valueOf(kv), idOf(kv)) }.takeWhile{ case(v,i) => endCondition(v) }.toStream
    }
  }
  */

  def getAllIds : Stream[Long] = {
    val iter = idIndexFile.iterator 
    iter.seekToFirst
    iter.asScala.map(_.getKey.as[Long]).toStream
  }

  def getAllValues : Stream[T] = {
    val iter = valIndexFile.iterator
    iter.seekToFirst
    iter.asScala.map(t => valueOf(t.getKey)).toStream.distinct
  }

  def valueOf(ab : Array[Byte]) : T = ab.take(ab.length - 8).as[T]
  def idOf(ab : Array[Byte]) : Long = ab.drop(ab.length - 8).as[Long]
}
