package com.precog.ragnarok


// final class IngestTest {
//   def run(config: RunConfig) {
//     import akka.actor.ActorSystem
//     import akka.dispatch.{ Future, ExecutionContext, Await }
//     import blueeyes.bkka.AkkaTypeClasses._
//     import PerfTestPrettyPrinters._
//     import RunConfig.OutputFormat
// 
//     val actorSystem = ActorSystem("perfTestingActorSystem")
//     try {
// 
//       implicit val execContext = ExecutionContext.defaultExecutionContext(actorSystem)
//       val testTimeout = Duration(120, "seconds")
// 
//       implicit val futureIsCopointed: Copointed[Future] = new Copointed[Future] {
//         def map[A, B](m: Future[A])(f: A => B) = m map f
//         def copoint[A](f: Future[A]) = Await.result(f, testTimeout)
//       }
// 
//       val runner = new JDBMPerfTestRunner(SimpleTimer,
//         optimize = config.optimize,
//         userUID = "dummy",
//         actorSystem = actorSystem,
//         _rootDir = config.rootDir)
// 
//       runner.startup()
// 
//       ////////
// 
//       config.ingest foreach { case (db, file) =>
//         runner.ingest(db, file).unsafePerformIO
//       }
// 
//       ///////
// 
//         run(test, runner, runs = config.dryRuns, outliers = config.outliers)
//         val result = run(test, runner, runs = config.runs, outliers = config.outliers) map {
//           case (t, stats) =>
//             (t, stats map (_ * (1 / 1000000.0))) // Convert to ms.
//         }
// 
//         config.baseline match {
//           case Some(file) =>
//             import java.io._
// 
//             val in = new FileInputStream(file)
//             using(in) { in =>
//               val reader = new InputStreamReader(in)
//               val baseline = BaselineComparisons.readBaseline(reader)
//               val delta = BaselineComparisons.compareWithBaseline(result, baseline)
// 
//               println(config.format match {
//                 case OutputFormat.Legible =>
//                   delta.toPrettyString
// 
//                 case OutputFormat.Json =>
//                   delta.toJson.toString
//               })
//             }
// 
//           case None =>
//             println(config.format match {
//               case OutputFormat.Legible =>
//                 result.toPrettyString
// 
//               case OutputFormat.Json =>
//                 result.toJson.toString
//             })
//         }
//       }
// 
//       runner.shutdown()
// 
//     } finally {
//       actorSystem.shutdown()
//     }
// }
// 
