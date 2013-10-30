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

import AssemblyKeys._

name := "niflheim"

fork := true

//run <<= inputTask { argTask =>
//  (javaOptions in run, fullClasspath in Compile, connectInput in run, outputStrategy, mainClass in run, argTask) map { (opts, cp, ci, os, mc, args) =>
//    val delim = java.io.File.pathSeparator
//    val opts2 = opts ++
//      Seq("-classpath", cp map { _.data } mkString delim) ++
//      Seq(mc.get) ++
//      args
//    Fork.java.fork(None, opts2, None, Map(), ci, os getOrElse StdoutOutput).exitValue()
//    jline.Terminal.getTerminal.initializeTerminal()
//  }
//}
//
//run in Test <<= inputTask { argTask =>
//  (javaOptions in run in Test, fullClasspath in Compile in Test, connectInput in run in Test, outputStrategy, mainClass in run in Test, argTask) map { (opts, cp, ci, os, mc, args) =>
//    val delim = java.io.File.pathSeparator
//    val opts2 = opts ++
//      Seq("-classpath", cp map { _.data } mkString delim) ++
//      Seq(mc.get) ++
//      args
//    Fork.java.fork(None, opts2, None, Map(), ci, os getOrElse StdoutOutput).exitValue()
//    jline.Terminal.getTerminal.initializeTerminal()
//  }
//}

//// For now, skip column specs because SBT will die a horrible, horrible death
//testOptions := Seq(Tests.Filter(s => ! s.contains("ColumnSpec")))

parallelExecution in test := false

libraryDependencies ++= Seq(
  //"commons-primitives"          %  "commons-primitives" % "1.0",
  //"net.sf.opencsv"              %  "opencsv"             % "2.0",
  //"ch.qos.logback"              %  "logback-classic"    % "1.0.0",
  //"com.typesafe.akka"           %  "akka-actor"         % "2.0.2",
  //"com.typesafe.akka"           %  "akka-testkit"       % "2.0.2" % "test",
  //"com.github.scopt"            %  "scopt_2.9.1"        % "2.0.1",
  //"org.apfloat"                 %  "apfloat"            % "1.6.3",
  "org.spire-math"              % "spire_2.9.1"              % "0.3.0-M2",
  "org.objectweb.howl"          %  "howl"               % "1.0.1-1"
)

//mainClass := Some("com.precog.yggdrasil.util.YggUtils")
//
//test in assembly := {}
