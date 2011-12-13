package reportgrid.storage.leveldb

import org.iq80.leveldb._
import org.fusesource.leveldbjni.JniDBFactory._
import java.io._
import java.math.BigDecimal
import Bijection._

import com.weiglewilczek.slf4s.Logger
import scalaz.Scalaz._
import scala.collection.JavaConverters._
import scala.collection.Iterator

class Column(name : String, dataDir : String) {
  val logger = Logger("col:" + name)

  private lazy val baseDir = {
    val bd = new File(dataDir,name)
    if (! bd.exists && ! bd.mkdirs()) {
      throw new IllegalStateException("Could not create the base directory: " + bd.getCanonicalPath)
    }
    bd
  }

  private val createOptions = (new Options).createIfMissing(true)
  private val idIndexFile =  factory.open(new File(baseDir, "idIndex"), createOptions)
  private val valIndexFile = factory.open(new File(baseDir, "valIndex"), createOptions)

  def close() {
    idIndexFile.close()
    valIndexFile.close()
  }

  def eval[T](db : DB)(f : DB => T): T = {
    f(db)
  }

  def insert(id : Long, v : BigDecimal) = {
    val idBytes = id.as[Array[Byte]]
    val valBytes = v.as[Array[Byte]]

    eval(idIndexFile) { _.put(idBytes, valBytes) } 
    eval(valIndexFile) { db =>
      val currentIds = Option(db.get(valBytes)).map(_.as[Iterable[Long]].toList).getOrElse(Nil)
                        
      db.put(valBytes, (id :: currentIds).sorted.toIterable.as[Array[Byte]])
    }
  }
  
  def getByRange(range: Interval[Long])(implicit ord: Ordering[Long]): Seq[(Long, BigDecimal)] = {
    import scala.math.Ordered._

    eval(idIndexFile) { db =>
      val iter = db.iterator
      range.start match {
        case Some(id) => iter.seek(id.as[Array[Byte]])
        case None => iter.seekToFirst
      }

      val endCondition = range.end match {
        case Some(id) => (l : Long) => l < id
        case None => (l : Long) => true
      }

      iter.asScala.map(kv => (kv.getKey.as[Long], kv.getValue.as[BigDecimal])).takeWhile(t => endCondition(t._1)).toSeq
    }
  }

  def getIds(v: BigDecimal): Seq[Long] = {
    eval(valIndexFile) { db =>
      Option(db.get(v.as[Array[Byte]])).map(_.as[Iterable[Long]].toSeq).getOrElse(Nil)
    }
  }

  def getAllIds : Iterator[Long] = eval(idIndexFile){ db =>
    new Iterator[Long] {
      private val iter = db.iterator 
      iter.seekToFirst
      def hasNext = iter.hasNext
      def next = iter.next.getKey.as[Long]
    }
  }

  def getAllValues : Iterator[BigDecimal] = eval(valIndexFile){ db =>
    new Iterator[BigDecimal] {
      private val iter = db.iterator
      iter.seekToFirst
      def hasNext = iter.hasNext
      def next = iter.next.getKey.as[BigDecimal]
    }
  }
}
