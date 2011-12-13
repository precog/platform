import sbt._
import Keys._

object PlatformBuild extends Build {
  lazy val platform = Project(id = "platform", base = file(".")) aggregate(quirrel, storage, bytecode)
  
  lazy val bytecode = Project(id = "bytecode", base = file("bytecode"))
  lazy val quirrel = Project(id = "quirrel", base = file("quirrel")) dependsOn bytecode
  lazy val storage = Project(id = "storage", base = file("storage"))
  
  lazy val daze = Project(id = "daze", base = file("daze")) dependsOn bytecode // (bytecode, storage)
}

