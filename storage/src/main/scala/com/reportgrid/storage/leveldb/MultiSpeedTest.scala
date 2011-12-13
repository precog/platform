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
package reportgrid.storage.leveldb

import org.scalacheck.Arbitrary
import akka.actor.Actor
import akka.dispatch.Future
import Actor._

import java.math._

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
    class DBActor(name : String, basedir : String)  extends Actor {
      private val column = new Column[BigDecimal](name, basedir)

      private var count = 0
      
      def receive = {
        case Insert(id,v) => column.insert(id,v); count += 1
        case KillMeNow => column.close(); self.tryReply(ShutdownComplete(name, count)); self.stop()
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
