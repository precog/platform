package com.precog.yggdrasil
package nihdb

import com.google.common.util.concurrent.ThreadFactoryBuilder

import scala.annotation.tailrec

import com.precog.common.accounts._
import com.precog.common.ingest._
import com.precog.common.security._
import com.precog.niflheim._
import com.precog.yggdrasil.table._
import com.precog.util.IOUtils

import akka.actor.{ActorSystem, Props}
import akka.dispatch.{Await, Future}
import akka.routing._
import akka.util.Duration

import blueeyes.akka_testing.FutureMatchers
import blueeyes.bkka.FutureMonad
import blueeyes.json._

import scalaz.NonEmptyList
import scalaz.effect.IO

import java.io._
import java.nio.ByteBuffer
import java.util.concurrent.ScheduledThreadPoolExecutor

class StressTest {
  import AsyncParser._

  val actorSystem = ActorSystem("NIHDBActorSystem")

  def makechef = new Chef(
    VersionedCookedBlockFormat(Map(1 -> V1CookedBlockFormat)),
    VersionedSegmentFormat(Map(1 -> V1SegmentFormat))
  )

  val chefs = (1 to 4).map { _ => actorSystem.actorOf(Props(makechef)) }

  val chef = actorSystem.actorOf(Props[Chef].withRouter(RoundRobinRouter(chefs)))

  val owner: AccountId = "account999"

  val authorities = Authorities(NonEmptyList(owner))

  val txLogScheduler = new ScheduledThreadPoolExecutor(10, (new ThreadFactoryBuilder()).setNameFormat("HOWL-sched-%03d").build())

  def newNihdb(workDir: File, threshold: Int = 1000): NIHDB =
    NIHDB.create(chef, authorities, workDir, threshold, Duration(60, "seconds"), txLogScheduler)(actorSystem).unsafePerformIO.valueOr { e => throw new Exception(e.message) }

  implicit val M = new FutureMonad(actorSystem.dispatcher)

  def shutdown() = actorSystem.shutdown()

  class TempContext {
    def fromFuture[A](f: Future[A]): A = Await.result(f, Duration(60, "seconds"))

    val workDir = IOUtils.createTmpDir("nihdbspecs").unsafePerformIO
    val nihdb = newNihdb(workDir)

    def close(proj: NIHDB) = fromFuture(proj.close(actorSystem))

    def finish() = {
        (for {
          _ <- IO { close(nihdb) }
          _ <- IOUtils.recursiveDelete(workDir)
        } yield ()).unsafePerformIO
    }

    def runNihAsync(i: Int, f: File, bufSize: Int, _eventid: Long): Long = {
      var t: Long = 0L
      def startit(): Unit = t = System.currentTimeMillis()

      def timeit(s: String) {
        val tt = System.currentTimeMillis()
        println("%s in %.3fs" format (s, (tt - t) * 0.001))
        t = tt
      }

      def timeit2(s: String) {
        val tt = System.currentTimeMillis()
        val d = (tt - t) * 0.001
        println("%s in %.3fs (%.3fs/M)" format (s, d, d / i))
        t = tt
      }

      var eventid: Long = _eventid

      startit()

      val ch = new FileInputStream(f).getChannel
      val bb = ByteBuffer.allocate(bufSize)

      @tailrec def loop(p: AsyncParser) {
        val n = ch.read(bb)
        bb.flip()

        val input = if (n >= 0) More(bb) else Done
        val (AsyncParse(errors, results), parser) = p(input)
        if (!errors.isEmpty) sys.error("errors: %s" format errors)
        //projection.insert(Array(eventid), results)
        val eventidobj = EventId.fromLong(eventid)
        nihdb.insert(Seq((eventid, results)))


        eventid += 1L
        bb.flip()
        if (n >= 0) loop(parser)
      }

      try {
        loop(AsyncParser(false))
      } finally {
        ch.close()
      }
      timeit("  finished ingesting")

      while (fromFuture(nihdb.status).pending > 0) Thread.sleep(100)
      timeit("  finished cooking")

      import scalaz._
      val length = NIHDBProjection.wrap(nihdb, authorities).flatMap { projection =>
        val stream = StreamT.unfoldM[Future, Unit, Option[Long]](None) { key =>
          projection.getBlockAfter(key, None).map(_.map { case BlockProjectionData(_, maxKey, _) => ((), Some(maxKey)) })
        }
        stream.length
      }

      Await.result(length, Duration(300, "seconds"))
      timeit2("  evaluated")

      eventid
    }
  }

 def main(args: Array[String]) {
    var eventid: Long = 1L
    val ctxt = new TempContext()
    val f = new File("yggdrasil/src/test/resources/z1m_nl.json")
    //val f = new File("yggdrasil/src/test/resources/z100k_nl.json")

    val t0 = System.currentTimeMillis()

    try {
      try {
        for (i <- 1 to 100) {
          println("iteration %d" format i)
          eventid = ctxt.runNihAsync(i, f, 8 * 1024 * 1024, eventid)
          val t = System.currentTimeMillis()
          println("total rows: %dM, total time: %.3fs" format (i, (t - t0) / 1000.0))
        }
      } finally {
        ctxt.finish()
      }
    } finally {
      shutdown()
    }
  }
}
