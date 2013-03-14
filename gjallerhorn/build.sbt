import AssemblyKeys._

name := "gjallerhorn"

fork := false

parallelExecution := false

libraryDependencies ++= Seq(
  "net.databinder.dispatch" %% "dispatch-core" % "0.9.5"
)

mainClass := Some("com.precog.gjallerhorn.RunAll")
