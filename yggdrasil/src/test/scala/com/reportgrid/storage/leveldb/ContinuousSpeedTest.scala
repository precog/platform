package com.reportgrid.storage
package leveldb

import com.reportgrid.util.Bijection
import com.reportgrid.yggdrasil.leveldb.LevelDBProjection

import org.iq80.leveldb._
import org.scalacheck.Arbitrary
import akka.actor.Actor
import akka.dispatch.Future
import Actor._

import java.io.File
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.util.concurrent.{CyclicBarrier,LinkedBlockingQueue}
import java.util.Random

import scala.math.Ordering

object ContinuousSpeedTest {
  def main (argv : Array[String]) {
    val (actorCount, chunkSize, basedir, seed, dataType) = argv match {
      case Array(ac,tc,bd,s,dtype) => (ac.toInt, tc.toInt, bd, s.toLong, dtype)
      case _ => {
        println("Usage: MultiSpeedTest <number of DBs> <chunk size> <base dir> <random seed>")
        sys.exit(1)
      }
    }
    
    val r = new java.util.Random(seed)

    class TestHarness[T: ({type X[a] = Bijection[a, Array[Byte]]})#X](dbGen : (String) => LevelDBProjection, valGen : => T) extends Runnable {
      sealed trait DBOp
      case class Insert(id : Long, value : T) extends DBOp
      case object Flush extends DBOp
    
      def run() {
        var startTime = System.currentTimeMillis
        var id : Long = 0
    
        val barrier = new CyclicBarrier(actorCount + 1, new Runnable {
          def run {
            val duration = System.currentTimeMillis - startTime
    
            println("%d\t%d\t%d\t%f".format(actorCount, id, duration, chunkSize / (duration / 1000.0)))
            startTime = System.currentTimeMillis
          }
        })
    
        // Spin up some processors
        class DBActor(name : String) extends Thread {
          private val column = dbGen(name)
    
          val queue = new java.util.concurrent.LinkedBlockingQueue[DBOp]()
          
          override def run {
            while (true) {
              queue.take match {
                case Insert(id, v) => column.insert(Vector(id), sys.error("todo")/*v.as[Array[Byte]].as[ByteBuffer]*/);
                case Flush => barrier.await()
              }
            }
          }
        }
    
        val processors = (1 to actorCount).map { actorId => (new DBActor("speed" + actorId)) }.toArray
    
        processors.foreach(_.start())
    
        // Just keep on inserting
        while (true) {
          var index = 0
    
          while (index < chunkSize) {
            processors(r.nextInt(actorCount)).queue.put(Insert(id, valGen))
            index += 1
            id += 1
          }
    
          processors.foreach{ p => p.queue.put(Flush) }
          barrier.await()
        }
      }
    }

    def column(n: String, comparator: DBComparator): LevelDBProjection = {
      LevelDBProjection(new File(basedir, n), sys.error("todo")/*Some(comparator)*/) ||| { errors =>
        errors.list.foreach(_.printStackTrace); sys.error("Could not obtain column.")
      }
    }
/*
    (dataType.toLowerCase match {
      case "long"    => sys.error("todo") //new TestHarness[Long](n => column(n, ProjectionComparator.Long), r.nextLong)
      case "decimal" => sys.error("todo") // new TestHarness[BigDecimal](n => column(n, ProjectionComparator.BigDecimal), BigDecimal.valueOf(r.nextDouble))
    }).run() */
  }
}
// vim: set ts=4 sw=4 et:
