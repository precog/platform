resolvers ++= Seq(
  "sbt-idea-repo" at "http://mpeltonen.github.com/maven/",
  "Scala-Tools Maven2 Snapshots Repository" at "http://scala-tools.org/repo-snapshots")

libraryDependencies ++= Seq( 
  Defaults.sbtPluginExtra("com.eed3si9n" % "sbt-assembly" % "0.7.2", "0.11.2", "2.9.1" ),
  Defaults.sbtPluginExtra("com.github.mpeltonen" % "sbt-idea" % "0.11.0", "0.11.2", "2.9.1"),
  Defaults.sbtPluginExtra("org.ensime" % "ensime-sbt-cmd" % "0.0.8", "0.11.2", "2.9.1")
)

addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "2.1.0")
