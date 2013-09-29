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
import Keys._
import AssemblyKeys._
import java.io.File
import scala.sys.process.{Process => SProcess, _}

name := "pandora"

javaOptions ++= Seq("-Xmx1G")

libraryDependencies ++= Seq(
  "org.sonatype.jline"      % "jline"       % "2.5"
)

mainClass := Some("com.precog.pandora.Console")

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
  def performExtract = {
    s.log.info("Extracting sample projection data into " + target.getCanonicalPath())
    val rc = SProcess("./direct-ingest.sh", Seq(target.getCanonicalPath)).!(new FileProcessLogger(new File("./target/test-gendata.log")))
    if (rc != 0) {
      error("Failed to extract data: " + rc)
    } else {
      s.log.info("Extraction complete.")
      target.getCanonicalPath
    }
  }  
  val back = if (!target.isDirectory()) {
    if (!target.mkdirs()) {
      error("Failed to make temp projection directory")
    } else {
      performExtract
    }
  } else {
    if (target.listFiles.length > 0) {
      s.log.info("Using data in " + target.getCanonicalPath())
      target.getCanonicalPath  
    } else {
      performExtract
    }
  }
  System.setProperty("precog.storage.root", back.toString)
  back
}

testOptions in Test <<= testOptions dependsOn extractData

parallelExecution in Test := false

fork in Test := true // required to avoid sigsegv with sbt 0.12

javaOptions in Test <<= extractData map { (target: String) =>
  Seq("-Xmx2G", "-XX:MaxPermSize=512m", "-Dprecog.storage.root=" + target)
} // required if using fork in Test

console in Compile <<= (console in Compile) dependsOn extractData

// The following is required because scct:test is ScctTest, not Test
testOptions in ScctTest <<= testOptions dependsOn extractData

parallelExecution in ScctTest := false

fork in ScctTest := true // required to avoid sigsegv with sbt 0.12

javaOptions in ScctTest <<= extractData map { (target: String) =>
  Seq("-Xmx2G", "-XX:MaxPermSize=512m", "-Dprecog.storage.root=" + target)
} // required if using fork in Test

initialCommands in console := """
  | import com.codecommit.gll.LineStream
  | 
  | import com.precog._
  |
  | import mimir._
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

