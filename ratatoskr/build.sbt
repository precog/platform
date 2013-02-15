import AssemblyKeys._

name := "ratatoskr"

libraryDependencies ++= Seq(
  "com.github.scopt"            %  "scopt_2.9.1"        % "2.0.1"
)

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

mainClass := Some("com.precog.ratatoskr.Ratatoskr")

test in assembly := {}
