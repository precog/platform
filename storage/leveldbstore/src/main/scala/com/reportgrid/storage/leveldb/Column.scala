/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.reportgrid.storage.leveldb

import org.iq80.leveldb._
import org.fusesource.leveldbjni.JniDBFactory._
import java.io._
import java.math.BigDecimal
import Bijection._

import com.weiglewilczek.slf4s.Logger
import scalaz.Scalaz._
import scala.collection.JavaConverters._
import scala.collection.Iterator

import reportgrid.analytics.Path
import blueeyes.json.JPath
import java.util.concurrent.CyclicBarrier
import scalaz.iteratee._

case class ColumnMetadata(path: Path, dataPath: JPath, storageType: String /* placeholder */) 

class Column[T](name : String, dataDir : String, metadata: ColumnMetadata)(implicit b : Bijection[T,Array[Byte]], comparator : Option[ColumnComparator[T]] = None) {
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
    def enumerator[A](iter: DBIterator): EnumeratorT[Unit, ByteBuffer, IO, A] = { s => 
      def terminate = s.pointI.map(a => iter.close; a)
      s.fold(
        cont = k => if (iter.hasNext) {
          val n = iter.next
          range.end match {
            case Some(end) if end <= n.getKey.as[Long] => s.pointI
            case _ => k(elInput(n)) >>== enumerator(iter)
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

      enumerator(iter)
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
