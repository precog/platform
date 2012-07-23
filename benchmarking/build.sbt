/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
name := "benchmarking"

version := "1.0.0"

libraryDependencies ++= Seq(
  "commons-io"                  %  "commons-io"         % "2.4",
  "com.weiglewilczek.slf4s"     %% "slf4s"              % "1.0.7",
  "ch.qos.logback"              %  "logback-classic"    % "1.0.0",
  "com.google.guava"            %  "guava"              % "12.0",
  "org.fusesource.leveldbjni"   %  "leveldbjni"         % "1.2-SNAPSHOT" changing(),
  "org.fusesource.leveldbjni"   %  "leveldbjni-osx"     % "1.2-SNAPSHOT" changing(),
  "org.fusesource.leveldbjni"   %  "leveldbjni-linux64" % "1.2-SNAPSHOT" changing()
)

resolvers ++= Seq(//"Local Maven Repository"            at "file://"+Path.userHome.absolutePath+"/.m2/repository",
                  "ReportGrid repo"                   at "http://nexus.reportgrid.com/content/repositories/releases",
                  "ReportGrid repo (public)"          at "http://nexus.reportgrid.com/content/repositories/public-releases",
                  "ReportGrid snapshot repo"          at "http://nexus.reportgrid.com/content/repositories/snapshots",
                  "ReportGrid snapshot repo (public)" at "http://nexus.reportgrid.com/content/repositories/public-snapshots",
                  "Typesafe Repository"               at "http://repo.typesafe.com/typesafe/releases/",
                  "Maven Repo 1"                      at "http://repo1.maven.org/maven2/",
                  "Guiceyfruit"                       at "http://guiceyfruit.googlecode.com/svn/repo/releases/",
                  "Sonatype Snapshots"                at "https://oss.sonatype.org/content/repositories/snapshots/")

credentials += Credentials(Path.userHome / ".ivy2" / ".rgcredentials")

mainClass := Some("com.precog.benchmarking.Benchmark")

run <<= inputTask { argTask =>
  (javaOptions in run, fullClasspath in Compile, connectInput in run, outputStrategy, mainClass in run, argTask) map { (opts, cp, ci, os, mc, args) =>
    val delim = java.io.File.pathSeparator
    val opts2 = opts ++
      Seq("-classpath", cp map { _.data } mkString delim) ++
      Seq(mc.get) ++
      (if (args.isEmpty) Seq() else args)
    Fork.java.fork(None, opts2, None, Map(), ci, os getOrElse StdoutOutput).exitValue()
    jline.Terminal.getTerminal.initializeTerminal()
  }
}

