//package com.precog.yggdrasil
//package nihdb
//
//import scala.annotation.tailrec
//
//import com.precog.niflheim._
//import com.precog.yggdrasil.table._
//import com.precog.util.IOUtils
//
//import akka.actor.{ActorSystem, Props}
//import akka.dispatch.{Await, Future}
//import akka.routing._
//import akka.util.Duration
//
//import blueeyes.akka_testing.FutureMatchers
//import blueeyes.bkka.FutureMonad
//import blueeyes.json._
//
//import scalaz.effect.IO
//
//import java.io._
//import java.nio.ByteBuffer
//
//class StressTest {
//  val actorSystem = ActorSystem("NIHDBActorSystem")
//
//  def makechef = new Chef(
//    VersionedCookedBlockFormat(Map(1 -> V1CookedBlockFormat)),
//    VersionedSegmentFormat(Map(1 -> V1SegmentFormat))
//  )
//
//  val chefs = (1 to 4).map { _ => actorSystem.actorOf(Props(makechef)) }
//
//  val chef = actorSystem.actorOf(Props[Chef].withRouter(RoundRobinRouter(chefs)))
//
//  def newNihProjection(workDir: File, threshold: Int = 1000) = new NIHDBProjection(workDir, null, chef, threshold, actorSystem, Duration(60, "seconds"))
//
//  implicit val M = new FutureMonad(actorSystem.dispatcher)
//
//  def shutdown() = actorSystem.shutdown()
//
//  class TempContext {
//    val workDir = IOUtils.createTmpDir("nihdbspecs").unsafePerformIO
//    var projection = newNihProjection(workDir)
//
//    def fromFuture[A](f: Future[A]): A = Await.result(f, Duration(60, "seconds"))
//
//    def close(proj: NIHDBProjection) = fromFuture(proj.close())
//
//    def finish() = {
//      (for {
//        _ <- IO { close(projection) }
//        _ <- IOUtils.recursiveDelete(workDir)
//      } yield ()).unsafePerformIO
//    }
//
//    def runNihAsync(i: Int, f: File, bufSize: Int, _eventid: Long): Long = {
//      var t: Long = 0L
//      def startit(): Unit = t = System.currentTimeMillis()
//
//      def timeit(s: String) {
//        val tt = System.currentTimeMillis()
//        println("%s in %.3fs" format (s, (tt - t) * 0.001))
//        t = tt
//      }
//
//      def timeit2(s: String) {
//        val tt = System.currentTimeMillis()
//        val d = (tt - t) * 0.001
//        println("%s in %.3fs (%.3fs/M)" format (s, d, d / i))
//        t = tt
//      }
//
//      var eventid: Long = _eventid
//
//      startit()
//
//      val ch = new FileInputStream(f).getChannel
//      val bb = ByteBuffer.allocate(bufSize)
//
//      @tailrec def loop(p: AsyncParser) {
//        val n = ch.read(bb)
//        bb.flip()
//
//        val input = if (n >= 0) Some(bb) else None
//        val (AsyncParse(errors, results), parser) = p(input)
//        if (!errors.isEmpty) sys.error("errors: %s" format errors)
//        projection.insert(Array(eventid), results)
//        eventid += 1L
//        bb.flip()
//        if (n >= 0) loop(parser)
//      }
//
//      try {
//        loop(AsyncParser())
//      } finally {
//        ch.close()
//      }
//  
//      while (fromFuture(projection.stats).pending > 0) Thread.sleep(100)
//      timeit("  finished cooking")
//
//      import scalaz._
//      val stream = StreamT.unfoldM[Future, Unit, Option[Long]](None) { key =>
//        projection.getBlockAfter(key).map(_.map { case BlockProjectionData(_, maxKey, _) => ((), Some(maxKey)) })
//      }
//
//      Await.result(stream.length, Duration(300, "seconds"))
//      timeit2("  evaluated")
//
//      eventid
//    }
//  }
//
//  def main(args: Array[String]) {
//    var eventid: Long = 1L
//    val ctxt = new TempContext()
//    val f = new File("yggdrasil/src/test/resources/z1m_nl.json")
//    //val f = new File("yggdrasil/src/test/resources/z100k_nl.json")
//
//    val t0 = System.currentTimeMillis()
//
//    try {
//      try {
//        for (i <- 1 to 100) {
//          println("iteration %d" format i)
//          eventid = ctxt.runNihAsync(i, f, 8 * 1024 * 1024, eventid)
//          val t = System.currentTimeMillis()
//          println("total rows: %dM, total time: %.3fs" format (i, (t - t0) / 1000.0))
//        }
//      } finally {
//        ctxt.finish()
//      }
//    } finally {
//      shutdown()
//    }
//  }
//}
