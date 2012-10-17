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

javaOptions ++= Seq("-Xmx1G")

libraryDependencies ++= Seq(
  "org.sonatype.jline"      % "jline"       % "2.5"
)

mainClass := Some("com.precog.pandora.Console")

//mainTest := "com.precog.pandora.TrampolinePlatformSpecs"

mainTest := "com.precog.pandora.FuturePlatformSpecs"

dataDir := {
  val file = File.createTempFile("pandora", ".db")
  file.delete()
  file.mkdir()
  file.getCanonicalPath
}
  
outputStrategy := Some(StdoutOutput)

connectInput in run := true
  
run <<= inputTask { argTask =>
  (javaOptions in run, fullClasspath in Compile, connectInput in run, outputStrategy, mainClass in run, argTask, extractData) map { (opts, cp, ci, os, mc, args, dataDir) =>
    val delim = java.io.File.pathSeparator
    val opts2 = opts ++
      Seq("-classpath", cp map { _.data } mkString delim) ++
      Seq(mc.get) ++
      (if (args.isEmpty) Seq(dataDir) else args)
    Fork.java.fork(None, opts2, None, Map(), ci, os getOrElse StdoutOutput).exitValue()
    jline.Terminal.getTerminal.initializeTerminal()
  }
}

extractData <<= (dataDir, streams) map { (dir, s) =>
  val target = new File(dir)
  val dataTarget = new File(target, "data")
  def performExtract = {
    s.log.info("Extracting sample projection data into " + target.getCanonicalPath())
    if (Process("./regen-jdbm-data.sh", Seq(dataTarget.getCanonicalPath)).! != 0) {
      error("Failed to extract data")
    } else {
      s.log.info("Extraction complete.")
      target.getCanonicalPath
    }
  }  
  if (!dataTarget.isDirectory()) {
    if (!dataTarget.mkdirs()) {
      error("Failed to make temp projection directory")
    } else {
      performExtract
    }
  } else {
    if (dataTarget.listFiles.length > 0) {
      s.log.info("Using data in " + target.getCanonicalPath())
      target.getCanonicalPath  
    } else {
      performExtract
    }
  }
}

definedTests in Test := Seq()

test <<= (streams, extractData, fullClasspath in Test, outputStrategy in Test, mainTest) map { (s, dataDir, cp, os, testName) =>
  val delim = java.io.File.pathSeparator
  val cpStr = cp map { _.data } mkString delim
  val opts2 =
    Seq("-Xmx1G") ++
    Seq("-classpath", cpStr) ++
    Seq("-Dprecog.storage.root=" + dataDir) ++
    (if (System.getProperty("sbt.log.noformat") == "true") Seq("-Dspecs2.color=false") else Seq()) ++
    Seq("-XX:+HeapDumpOnOutOfMemoryError") ++
    Seq("specs2.run") ++
    Seq(testName)
  s.log.debug("Running with flags: " + opts2.mkString(" "))
  val result = Fork.java.fork(None, opts2, None, Map(), false, LoggedOutput(s.log)).exitValue()
  if (result != 0) error("Tests unsuccessful")    // currently has no effect (https://github.com/etorreborre/specs2/issues/55)
}

(console in Compile) <<= (streams, extractData, initialCommands in console, fullClasspath in Compile, scalaInstance) map { (s, dataDir, init, cp, si) =>
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
  | import com.codecommit.gll.LineStream
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
  | import yggdrasil.actor._
  | import SBTConsole.platform
  | """.stripMargin
  
logBuffered := false       // gives us incremental output from Specs2

dist <<= (version, streams, baseDirectory, target in assembly, jarName in assembly) map { 
         (projectVersion: String, streams: TaskStreams, projectRoot: File, buildTarget: File, assemblyName: String) => {
  val log = streams.log
  val distStaticRoot = new File(projectRoot, "dist")
  val distName = "pandist-eap".format(projectVersion)
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

