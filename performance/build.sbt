// 
//  ____    ____    _____    ____    ___     ____ 
// |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
// | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
// |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
// |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
// 
// This program is free software: you can redistribute it and/or modify it under the terms of the 
// GNU Affero General Public License as published by the Free Software Foundation, either version 
// 3 of the License, or (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
// without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
// the GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License along with this 
// program. If not, see <http://www.gnu.org/licenses/>.
// 
// 

import sbt._
import Keys._
import AssemblyKeys._
import java.io.File

name := "performance"

mainTest := "com.precog.performance.PerformanceSuite"

test <<= (streams, fullClasspath in Test, outputStrategy in Test, mainTest) map { (s, cp, os, testName) =>
  val delim = java.io.File.pathSeparator
  val cpStr = cp map { _.data } mkString delim
  s.log.debug("Running with classpath: " + cpStr)
  val opts2 =
    Seq("-server") ++
    Seq("-XX:MaxPermSize=512m") ++
    Seq("-Xms3G") ++
    Seq("-Xmx4G") ++
    Seq("-XX:+UseConcMarkSweepGC") ++
    Seq("-XX:+CMSIncrementalMode") ++
    Seq("-XX:-CMSIncrementalPacing") ++ 
    Seq("-XX:CMSIncrementalDutyCycle=100") ++
    Seq("-classpath", cpStr) ++
    Seq("specs2.run") ++
    Seq(testName)
  val result = Fork.java.fork(None, opts2, None, Map(), false, LoggedOutput(s.log)).exitValue()
  if (result != 0) error("Tests unsuccessful")    // currently has no effect (https://github.com/etorreborre/specs2/issues/55)
}
