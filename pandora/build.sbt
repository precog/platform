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
  s.log.info("Extracting LevelDB sample data...")
  IO.copyDirectory(new File("pandora/dist/data/"), new File("/tmp/pandora/data/"), true, false)
}

extractLibs <<= (streams, fullClasspath in Compile) map { (s, cp) =>
  val path = new File("/tmp/leveldbjni")
  if (!path.exists()) {
    s.log.info("Extracting LevelDB native libraries...")
    path.mkdir()
    for {
      key <- cp
      val file = key.data
      if file.getName contains "leveldbjni"
    } {
      IO.unzip(file, path)      // TODO filter
    }
  }
  val libPath = System.getProperty("java.library.path")
  if (!libPath.contains("/tmp/leveldbjni")) {
    val native = new File("/tmp/leveldbjni/META-INF/native/")
    for (subdir <- native.listFiles) {
      s.log.info("Adding " + subdir.getCanonicalPath + " to java.library.path")
      System.setProperty("java.library.path", System.getProperty("java.library.path") + File.pathSeparator + subdir.getCanonicalPath)
    }
  }
}

test <<= test dependsOn extractLibs

testOnly <<= inputTask { argTask =>
  streams map { s =>
    error("test-only is currently not supported due to SBT insanity")
  }
}

(console in Compile) <<= (console in Compile) dependsOn extractLibs

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
  | net.lag.configgy.Configgy.configureFromResource("default_ingest.conf")
  |
  | val platform = new Parser
  |                  with LineErrors
  |                  with TreeShaker
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
  |     // start ingest server
  |     Await.result(start, controlTimeout)
  |     // start storage shard 
  |     Await.result(storage.start, controlTimeout)
  |   }
  |   
  |   def shutdown() {
  |     // stop storaget shard
  |     Await.result(storage.stop, controlTimeout)
  |     // stop ingest server
  |     Await.result(stop, controlTimeout)
  |     
  |     actorSystem.shutdown
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

