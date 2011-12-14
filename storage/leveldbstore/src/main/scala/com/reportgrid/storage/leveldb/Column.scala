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

//import reportgrid.analytics.Path
//import blueeyes.json.JPath
import java.util.concurrent.CyclicBarrier

import scalaz.effect._
import scalaz.iteratee._
import scalaz.iteratee.Input._
import scalaz.Scalaz._

//case class ColumnMetadata(path: Path, dataPath: JPath, storageType: String /* placeholder */) 
case class ColumnMetadata(path: String, dataPath: String, storageType: String)

class Column[T](name : String, dataDir : String)(implicit b : Bijection[T,Array[Byte]], comparator : Option[ColumnComparator[T]] = None) {
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

  def eval[A](db : DB)(f : DB => A): A = {
    f(db)
  }

  private def shouldSync : Boolean = {
    false
  }

  def insert(id : Long, v : T) = {
    val idBytes = id.as[Array[Byte]]
    val valBytes = v.as[Array[Byte]]

    eval(idIndexFile) { _.put(idBytes, valBytes) } 
    eval(valIndexFile) { _.put(valBytes ++ idBytes, Array[Byte]()) }
  }
  
  def getValuesByIdRange[A](range: Interval[Long]): EnumeratorT[Unit, ByteBuffer, IO, A] = {
    def enumerator[A](iter: DBIterator, close: IO[Unit]): EnumeratorT[Unit, ByteBuffer, IO, A] = { s => 
      s.fold(
        cont = k => if (iter.hasNext) {
          val n = iter.next
          range.end match {
            case Some(end) if end <= n.getKey.as[Long] => s.pointI
            case _ => k(elInput(ByteBuffer.wrap(n.getValue))) >>== enumerator(iter, close)
          }
        } else {
          s.pointI
        },
        done = (_, _) => s.pointI,
        err = _ => s.pointI
      )
    }
    
    eval(idIndexFile) { db =>
      val iter = db.iterator
      range.start match {
        case Some(id) => iter.seek(id.as[Array[Byte]])
        case None => iter.seekToFirst
      }

      enumerator(iter, IO(iter.close))
    }
  }

  def getIds(v : T)(implicit o : Ordering[T]): Stream[Long] = {
    import scala.math.Ordered._

    eval (valIndexFile) { db =>
      val iter = db.iterator
      iter.seek(v.as[Array[Byte]] ++ 0L.as[Array[Byte]])
      iter.asScala.map(i => i.getKey).takeWhile(kv => valueOf(kv) == v).map(idOf).toStream
    }
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

  def getAllIds : Stream[Long] = eval(idIndexFile){ db =>
    val iter = db.iterator 
    iter.seekToFirst
    iter.asScala.map(_.getKey.as[Long]).toStream
  }

  def getAllValues : Stream[T] = eval(valIndexFile){ db =>
    val iter = db.iterator
    iter.seekToFirst
    iter.asScala.map(t => valueOf(t.getKey)).toStream.distinct
  }

  def valueOf(ab : Array[Byte]) : T = ab.take(ab.length - 8).as[T]
  def idOf(ab : Array[Byte]) : Long = ab.drop(ab.length - 8).as[Long]
}
