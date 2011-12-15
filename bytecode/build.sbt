name := "bytecode"

organization := "com.querio"

version := "0.1.0"

scalaVersion := "2.9.1"

resolvers += "Scala-Tools Maven2 Snapshots Repository" at "http://scala-tools.org/repo-snapshots"

libraryDependencies ++= Seq(
  "org.scala-tools.testing" %% "scalacheck" % "1.9" % "test" withSources,
  "org.specs2" %% "specs2" % "1.7-SNAPSHOT" % "test" withSources)

logBuffered := false

publishArtifact in packageDoc := false

initialCommands in console := """
  | import com.querio.bytecode._
  | import java.nio.ByteBuffer
  | 
  | val cake = new Reader with BytecodeWriter
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
