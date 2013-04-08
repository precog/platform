import AssemblyKeys._

name := "gjallerhorn"

fork := false

parallelExecution := false

libraryDependencies ++= Seq(
  "org.scalacheck"              %% "scalacheck"         % "1.10.0",
  "org.specs2"                  %% "specs2"             % "1.12.3",
  "net.databinder.dispatch" %% "dispatch-core" % "0.9.5"
)

mainClass := Some("com.precog.gjallerhorn.RunAll")
