import AssemblyKeys._

name := "yggdrasil"

fork := true

run <<= inputTask { argTask =>
  (javaOptions in run, fullClasspath in Compile, connectInput in run, outputStrategy, mainClass in run, argTask) map { (opts, cp, ci, os, mc, args) =>
    val delim = java.io.File.pathSeparator
    val opts2 = opts ++
      Seq("-classpath", cp map { _.data } mkString delim) ++
      Seq(mc.get) ++
      args
    Fork.java.fork(None, opts2, None, Map(), ci, os getOrElse StdoutOutput).exitValue()
    jline.Terminal.getTerminal.initializeTerminal()
  }
}

run in Test <<= inputTask { argTask =>
  (javaOptions in run in Test, fullClasspath in Compile in Test, connectInput in run in Test, outputStrategy, mainClass in run in Test, argTask) map { (opts, cp, ci, os, mc, args) =>
    val delim = java.io.File.pathSeparator
    val opts2 = opts ++
      Seq("-classpath", cp map { _.data } mkString delim) ++
      Seq(mc.get) ++
      args
    Fork.java.fork(None, opts2, None, Map(), ci, os getOrElse StdoutOutput).exitValue()
    jline.Terminal.getTerminal.initializeTerminal()
  }
}

// For now, skip column specs because SBT will die a horrible, horrible death
testOptions := Seq(Tests.Filter(s => ! s.contains("ColumnSpec")))

parallelExecution in test := false

libraryDependencies ++= Seq(
  "commons-primitives"          %  "commons-primitives" % "1.0",
  "net.sf.opencsv"              %  "opencsv"             % "2.0",
  "ch.qos.logback"              %  "logback-classic"    % "1.0.0",
  "com.typesafe.akka"           %  "akka-actor"         % "2.0.2",
  "com.typesafe.akka"           %  "akka-testkit"       % "2.0.2" % "test",
  "com.github.scopt"            %  "scopt_2.9.1"        % "2.0.1",
  "org.apfloat"                 %  "apfloat"            % "1.6.3"
)

mainClass := Some("com.precog.yggdrasil.util.YggUtils")

test in assembly := {}
