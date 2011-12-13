package reportgrid.storage.leveldb

import org.scalacheck.Arbitrary
import akka.actor.Actor
import akka.dispatch.Future
import Actor._

import java.math._

import java.util.concurrent.{CyclicBarrier,LinkedBlockingQueue}

object ContinuousSpeedTest {
  sealed trait DBOp
  case class Insert(id : Long, value : BigDecimal) extends DBOp
  case object Flush extends DBOp

  def main (argv : Array[String]) {
    val (actorCount, chunkSize, basedir, seed) = argv match {
      case Array(ac,tc,bd,s) => (ac.toInt, tc.toInt, bd, s.toLong)
      case _ => {
        println("Usage: MultiSpeedTest <number of DBs> <chunk size> <base dir> <random seed>")
        sys.exit(1)
      }
    }

    var startTime = System.currentTimeMillis
    val r = new java.util.Random()
    var id : Long = 0

    val barrier = new CyclicBarrier(actorCount + 1, new Runnable {
      def run {
        val duration = System.currentTimeMillis - startTime

        println("%d\t%d\t%d\t%f".format(actorCount, id, duration, chunkSize / (duration / 1000.0)))
        startTime = System.currentTimeMillis
      }
    })

    // Spin up some processors
    class DBActor(name : String, basedir : String) extends Thread {
      private val column = new Column(name, basedir)

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

    val processors = (1 to actorCount).map { actorId => (new DBActor("speed" + actorId, basedir)) }.toArray

    processors.foreach(_.start())

    // Just keep on inserting
    while (true) {
      var index = 0

      while (index < chunkSize) {
        processors(r.nextInt(actorCount)).queue.put(Insert(id, BigDecimal.valueOf(r.nextDouble)))
        index += 1
        id += 1
      }

      processors.foreach{ p => p.queue.put(Flush) }
      barrier.await()
    }
  }
}

// vim: set ts=4 sw=4 et:
