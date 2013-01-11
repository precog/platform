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
  val dataTarget = new File(target, "data")
  def performExtract = {
    s.log.info("Extracting sample projection data into " + target.getCanonicalPath())
    val rc = SProcess("./regen-jdbm-data.sh", Seq(dataTarget.getCanonicalPath)).!(new FileProcessLogger(new File("./target/test-gendata.log")))
    if (rc != 0) {
      error("Failed to extract data: " + rc)
    } else {
      s.log.info("Extraction complete.")
      target.getCanonicalPath
    }
  }  
  val back = if (!dataTarget.isDirectory()) {
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
  System.setProperty("precog.storage.root", back.toString)
  back
}

testOptions in Test <<= testOptions dependsOn extractData

parallelExecution in Test := false

console in Compile <<= (console in Compile) dependsOn extractData

initialCommands in console := """
  | import com.codecommit.gll.LineStream
  | 
  | import com.precog._
  |
  | import daze._
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

