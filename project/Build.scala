import sbt._
import Keys._

object PlatformBuild extends Build {
  lazy val platform = Project(id = "platform", base = file(".")) aggregate(quirrel, storage)
  lazy val quirrel = Project(id = "quirrel", base = file("quirrel"))
  lazy val storage = Project(id = "storage", base = file("storage/leveldbstore"))
}

