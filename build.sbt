import sbt._
import Keys._

(test in Test) <<= (test in Test, test in LocalProject("pandora")) map { (_, _) => () }
