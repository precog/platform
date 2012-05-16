package com.precog.storage
package leveldb
import com.precog.common.VectorCase
import com.precog.util.Bijection
import com.precog.yggdrasil.leveldb.LevelDBProjection

import org.scalacheck.Arbitrary
import akka.util.Timeout
import akka.util.duration._
import akka.actor.{Actor, ActorSystem, PoisonPill, Props}
import akka.dispatch.Future
import akka.dispatch.Await
import akka.pattern.ask
import Actor._

import java.io.File
import java.math.BigDecimal
import java.nio.ByteBuffer

import org.scalacheck.Arbitrary

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
    implicit val actorTimeout: Timeout = 5 minutes

    // Spin up some actor
    class DBActor(name : String, basedir : String) extends Actor {
      private val column = LevelDBProjection.forDescriptor(new File(basedir, name), sys.error("todo") /*Some(ProjectionComparator.BigDecimal)*/) ||| {
        errors => throw errors
      }

      private var count = 0
      
      def receive = {
        case Insert(id,v) => column.insert(VectorCase(id), sys.error("todo")/*v.as[Array[Byte]].as[ByteBuffer]*/).map(_ => count += 1).unsafePerformIO

        case KillMeNow => column.close.map(_ => sender ? (ShutdownComplete(name, count))).map(_ => self ! PoisonPill).unsafePerformIO
      }
    }

    implicit val actorSystem = ActorSystem("SpeedTest")

    val actors = (1 to actorCount).map { actorId => actorSystem.actorOf(Props(new DBActor("speed" + actorId, basedir))) }.toArray

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
    
    Await.result(Future.sequence(results), 30 seconds)

    val duration = System.currentTimeMillis - startTime

    //println("%d insertions in %d ms (%f per second)".format(totalCount, duration, totalCount / (duration / 1000.0)))
    println("%d,%d,%d".format(actorCount, totalCount, duration))
  }
}

// vim: set ts=4 sw=4 et:
