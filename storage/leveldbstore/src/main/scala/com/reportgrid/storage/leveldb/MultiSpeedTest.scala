package com.reportgrid.storage.leveldb

import comparators._
import Bijection._

import org.scalacheck.Arbitrary
import akka.actor.Actor
import akka.dispatch.Future
import Actor._

import java.io.File
import java.math.BigDecimal
import java.nio.ByteBuffer

object MultiSpeedTest {
  case class Insert(id : Long, value : BigDecimal)
  case object KillMeNow
  case class ShutdownComplete(name : String, totalInserts : Int)

  def main (argv : Array[String]) {
    val (actorCount, totalCount, basedir, seed) = argv match {
      case Array(ac,tc,bd,s) => (ac.toInt, tc.toInt, bd, s.toLong)
      case _ => {
        println("Usage: MultiSpeedTest <number of DBs> <total insertions> <base dir> <random seed>")
        sys.exit(1)
      }
    }

    // We can wait up to 5 minutes
    implicit val actorTimeout = Timeout(300000)

    // Spin up some actor
    class DBActor(name : String, basedir : String) extends Actor {
      private val column = new Column(new File(basedir, name), ColumnComparator.BigDecimal)

      private var count = 0
      
      def receive = {
        case Insert(id,v) => column.insert(id, v.as[Array[Byte]].as[ByteBuffer]).map(_ => count += 1).unsafePerformIO

        case KillMeNow => column.close.map(_ => self.tryReply(ShutdownComplete(name, count))).map(_ => self.stop()).unsafePerformIO
      }
    }

    val actors = (1 to actorCount).map { actorId => actorOf(new DBActor("speed" + actorId, basedir)).start }.toArray

    // Create some random values to insert
    val r = new java.util.Random(seed)
    val values = Array.fill(8192)(BigDecimal.valueOf(r.nextDouble))

    var id : Long = 0

    val startTime = System.currentTimeMillis

    while (id < totalCount) {
      actors(r.nextInt(actorCount)) ! Insert(id, values(r.nextInt(8192)))
      id += 1
    }

    val results : List[Future[Any]] = actors.map{ a => a ? KillMeNow  }.toList
    Future.sequence(results, 300000).get

    val duration = System.currentTimeMillis - startTime

    //println("%d insertions in %d ms (%f per second)".format(totalCount, duration, totalCount / (duration / 1000.0)))
    println("%d,%d,%d".format(actorCount, totalCount, duration))
  }
}

// vim: set ts=4 sw=4 et:
