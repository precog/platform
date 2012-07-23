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

