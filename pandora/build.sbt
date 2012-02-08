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
  "org.specs2" %% "specs2" % "1.7" % "test",
  "org.scala-tools.testing" %% "scalacheck" % "1.9")
  
outputStrategy := Some(StdoutOutput)

connectInput in run := true
  
fork in run := true

run <<= inputTask { argTask =>
  (javaOptions in run, fullClasspath in Compile, connectInput in run, outputStrategy, mainClass in run, argTask) map { (opts, cp, ci, os, mc, args) =>
    val delim = java.io.File.pathSeparator
    val opts2 = opts ++
      Seq("-classpath", cp map { _.data } mkString delim) ++
      Seq(mc getOrElse "com.precog.pandora.Console") ++
      Seq("/tmp/pandora/data/") ++ 
      args
    Fork.java.fork(None, opts2, None, Map(), ci, os getOrElse StdoutOutput).exitValue()
    jline.Terminal.getTerminal.initializeTerminal()
  } dependsOn extractData
}

extractData <<= streams map { s =>
  val target = new File("/tmp/pandora/data/")
  if (!target.exists()) {
    s.log.info("Extracting LevelDB sample data...")
    IO.copyDirectory(new File("pandora/dist/data/"), target, true, false)
  }
}

test <<= (streams, fullClasspath in Test, outputStrategy in Test) map { (s, cp, os) =>
  val delim = java.io.File.pathSeparator
  val cpStr = cp map { _.data } mkString delim
  s.log.debug("Running with classpath: " + cpStr)
  val opts2 =
    Seq("-classpath", cpStr) ++
    Seq("-Dpandora.data=/tmp/pandora/data") ++
    Seq("specs2.run") ++
    Seq("com.precog.pandora.PlatformSpecs")
  val result = Fork.java.fork(None, opts2, None, Map(), false, os getOrElse StdoutOutput).exitValue()
  if (result != 0) error("Tests unsuccessful")    // currently has no effect (https://github.com/etorreborre/specs2/issues/55)
} dependsOn extractData

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
  | val platform = new Compiler
  |                  with LineErrors
  |                  with ProvenanceChecker
  |                  with Emitter
  |                  with Evaluator
  |                  with DatasetConsumers 
  |                  with YggdrasilOperationsAPI
  |                  with YggdrasilStorage
  |                  with AkkaIngestServer
  |                  with DefaultYggConfig {
  |   
  |   import akka.dispatch.Await
  |   import akka.util.Duration
  |
  |   import java.io.File
  |
  |   val controlTimeout = Duration(120, "seconds")
  |   lazy val storageRoot = new File("/tmp/pandora/data")
  | 
  |   def startup() {
  |     // start storage shard 
  |     Await.result(storage.start, controlTimeout)
  |   }
  |   
  |   def shutdown() {
  |     // stop storaget shard
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

