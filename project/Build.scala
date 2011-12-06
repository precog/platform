import sbt._
import Keys._

object PlatformBuild extends Build {
  lazy val platform = Project(id = "platform", base = file(".")) aggregate(quirrel)
  lazy val quirrel = Project(id = "quirrel", base = file("quirrel"))
}

