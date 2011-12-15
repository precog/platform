package com.reportgrid.storage.leveldb

import org.scalacheck.Arbitrary
import akka.actor.Actor
import akka.dispatch.Future
import Actor._

import java.math._

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

    class TestHarness[T](dbGen : (String) => Column[T], valGen : => T) extends Runnable {
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
                case Insert(id,v) => column.insert(id,v);
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

    (dataType.toLowerCase match {
      case "long" => new TestHarness[Long](new Column(_,basedir,Ordering.Long), r.nextLong)
      case "decimal" => new TestHarness[BigDecimal](new Column(_,basedir,JBigDecimalOrdering), BigDecimal.valueOf(r.nextDouble))
    }).run()
  }
}

// vim: set ts=4 sw=4 et:
