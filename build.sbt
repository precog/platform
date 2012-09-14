import sbt._
import Keys._

parallelExecution := false

(test in Test) <<= (test in Test, test in LocalProject("pandora")) map { (_, _) => () }
