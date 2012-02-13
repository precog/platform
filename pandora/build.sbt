import sbt._
import Keys._
import AssemblyKeys._
import java.io.File

name := "pandora"

version := "0.0.1-SNAPSHOT"

organization := "com.precog"

scalaVersion := "2.9.1"

scalacOptions ++= Seq("-deprecation", "-unchecked")

libraryDependencies ++= Seq(
  "org.sonatype.jline" % "jline" % "2.5",
  "org.specs2" %% "specs2" % "1.8-SNAPSHOT" % "test" changing(),
  "org.scala-tools.testing" %% "scalacheck" % "1.9")
  
outputStrategy := Some(StdoutOutput)

connectInput in run := true
  
fork in run := true

run <<= inputTask { argTask =>
  (javaOptions in run, fullClasspath in Compile, connectInput in run, outputStrategy, mainClass in run, argTask, extractData) map { (opts, cp, ci, os, mc, args, dataDir) =>
    val delim = java.io.File.pathSeparator
    val opts2 = opts ++
      Seq("-classpath", cp map { _.data } mkString delim) ++
      Seq(mc getOrElse "com.precog.pandora.Console") ++
      (if (args.isEmpty) Seq(dataDir) else args)
    Fork.java.fork(None, opts2, None, Map(), ci, os getOrElse StdoutOutput).exitValue()
    jline.Terminal.getTerminal.initializeTerminal()
  }
}

extractData <<= streams map { s =>
  val target = new File("/tmp/pandora/data/")
  if (!target.exists()) {
    s.log.info("Extracting LevelDB sample data...")
    IO.copyDirectory(new File("pandora/dist/data/"), target, true, false)
  }
  target.getCanonicalPath
}

definedTests in Test ~= { tests => tests filter { _.name != "com.precog.pandora.PlatformSpecs" } }

test <<= (streams, fullClasspath in Test, outputStrategy in Test, extractData) map { (s, cp, os, dataDir) =>
  val delim = java.io.File.pathSeparator
  val cpStr = cp map { _.data } mkString delim
  s.log.debug("Running with classpath: " + cpStr)
  val opts2 =
    Seq("-classpath", cpStr) ++
    Seq("-Dprecog.storage.root=" + dataDir) ++
    Seq("specs2.run") ++
    Seq("com.precog.pandora.PlatformSpecs")
  val result = Fork.java.fork(None, opts2, None, Map(), false, os getOrElse StdoutOutput).exitValue()
  if (result != 0) error("Tests unsuccessful")    // currently has no effect (https://github.com/etorreborre/specs2/issues/55)
}

(console in Compile) <<= (streams, initialCommands in console, fullClasspath in Compile, scalaInstance, extractData) map { (s, init, cp, si, dataDir) =>
  IO.withTemporaryFile("pandora", ".scala") { file =>
    IO.write(file, init)
    val delim = java.io.File.pathSeparator
    val scalaCp = (si.compilerJar +: si.extraJars) map { _.getCanonicalPath }
    val fullCp = (cp map { _.data }) ++ scalaCp
    val cpStr = fullCp mkString delim
    s.log.debug("Running with classpath: " + cpStr)
    val opts2 =
      Seq("-classpath", cpStr) ++
      Seq("-Dscala.usejavacp=true") ++
      Seq("-Dprecog.storage.root=" + dataDir) ++
      Seq("scala.tools.nsc.MainGenericRunner") ++
      Seq("-Yrepl-sync", "-i", file.getCanonicalPath)
    Fork.java.fork(None, opts2, None, Map(), true, StdoutOutput).exitValue()
    jline.Terminal.getTerminal.initializeTerminal()
  }
}

initialCommands in console := """
  | import edu.uwm.cs.gll.LineStream
  | 
  | import com.precog._
  |
  | import daze._
  | import daze.util._
  | 
  | import pandora._
  | 
  | import quirrel._
  | import quirrel.emitter._
  | import quirrel.parser._
  | import quirrel.typer._
  | 
  | import yggdrasil._
  | import yggdrasil.shard._
  | 
  | val platform = new Compiler with LineErrors with ProvenanceChecker with Emitter with Evaluator with DatasetConsumers with OperationsAPI with AkkaIngestServer with YggdrasilEnumOpsComponent with LevelDBQueryComponent {
  |   import akka.dispatch.Await
  |   import akka.util.Duration
  |   import scalaz._
  |
  |   import java.io.File
  |
  |   import scalaz.effect.IO
  |   
  |   import org.streum.configrity.Configuration
  |   import org.streum.configrity.io.BlockFormat
  | 
  |   val controlTimeout = Duration(30, "seconds")
  |
  |   type YggConfig = YggEnumOpsConfig with LevelDBQueryConfig
  |   lazy val yggConfig = loadConfig(Option(System.getProperty("precog.storage.root"))).unsafePerformIO
  |
  |   val maxEvalDuration = controlTimeout
  |
  |   val Success(shardState) = YggState.restore(yggConfig.dataDir).unsafePerformIO
  |   
  |   object storage extends ActorYggShard {
  |     val yggState = shardState 
  |   }
  |   
  |   object ops extends Ops 
  |   
  |   object query extends QueryAPI 
  |
  |   def loadConfig(dataDir: Option[String]): IO[BaseConfig with YggEnumOpsConfig with LevelDBQueryConfig] = IO {
  |     val rawConfig = dataDir map { "precog.storage.root = " + _ } getOrElse { "" }
  | 
  |     new BaseConfig with YggEnumOpsConfig with LevelDBQueryConfig {
  |       val config = Configuration.parse(rawConfig)  
  |       val flatMapTimeout = controlTimeout
  |       val projectionRetrievalTimeout = akka.util.Timeout(controlTimeout)
  |     }
  |   }
  |
  |   def eval(str: String): Set[SValue] = evalE(str) map { _._2 }
  | 
  |   def evalE(str: String) = {
  |     val tree = compile(str)
  |     if (!tree.errors.isEmpty) {
  |       sys.error(tree.errors map showError mkString ("Set(\"", "\", \"", "\")"))
  |     }
  |     val Right(dag) = decorate(emit(tree))
  |     consumeEval(dag)
  |   }
  | 
  |   def startup() {
  |     // start storage shard 
  |     Await.result(storage.start, controlTimeout)
  |   }
  |   
  |   def shutdown() {
  |     // stop storage shard
  |     Await.result(storage.stop, controlTimeout)
  |     
  |     actorSystem.shutdown()
  |   }
  | }""".stripMargin
  
logBuffered := false       // gives us incremental output from Specs2

mainClass := Some("com.precog.pandora.Console")

dist <<= (version, streams, baseDirectory, target in assembly, jarName in assembly) map { 
         (projectVersion: String, streams: TaskStreams, projectRoot: File, buildTarget: File, assemblyName: String) => {
  val log = streams.log
  val distStaticRoot = new File(projectRoot, "dist")
  val distName = "pandist-%s".format(projectVersion)
  val distTmp = new File(buildTarget, distName)
  val distTarball = new File(buildTarget, distName + ".tar.gz")
  val assemblyJar = new File(buildTarget, assemblyName)
  val distTmpLib = new File(distTmp, "lib/") 
  log.info("copy static dist contents")
  List("cp", "-r", distStaticRoot.toString, distTmp.toString) ! log
  log.info("copy assembly jar: %s".format(assemblyName)) 
  distTmpLib.mkdirs
  List("cp", assemblyJar.toString, distTmpLib.toString) ! log
  log.info("create tarball")
  List("tar", "-C", distTmp.getParent.toString, "-cvzf", distTarball.toString, distTmp.getName) ! log
}}

dist <<= dist.dependsOn(assembly)

