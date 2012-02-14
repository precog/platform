name := "bytecode"

organization := "com.precog"

version := "0.1.0"

scalaVersion := "2.9.1"

resolvers += "Scala-Tools Maven2 Snapshots Repository" at "http://scala-tools.org/repo-snapshots"

libraryDependencies ++= Seq(
  "org.scala-tools.testing" %% "scalacheck" % "1.9" % "test" withSources,
  "org.scalaz" %% "scalaz-core" % "7.0-SNAPSHOT" changing(),
  "org.specs2" %% "specs2" % "1.8" % "test" withSources())

logBuffered := false

publishArtifact in packageDoc := false

initialCommands in console := """
  | import com.precog.bytecode._
  | import java.nio.ByteBuffer
  | 
  | val cake = new BytecodeReader with BytecodeWriter with DAG with util.DAGPrinter
  | 
  | def printBuffer(buffer: ByteBuffer) {
  |   try {
  |     val b = buffer.get()
  |     printf("%02X ", b)
  |   } catch {
  |     case _ => return
  |   }
  |   printBuffer(buffer)
  | }""".stripMargin
