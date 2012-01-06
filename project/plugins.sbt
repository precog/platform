resolvers ++= Seq(
  "Condep repo" at "http://samskivert.github.com/sbt-condep-plugin/maven",
  "sbt-idea-repo" at "http://mpeltonen.github.com/maven/",
  "Scala-Tools Maven2 Snapshots Repository" at "http://scala-tools.org/repo-snapshots")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.7.2")

addSbtPlugin("com.samskivert" %% "sbt-condep-plugin" % "1.1")

addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "0.11.0")

addSbtPlugin("org.ensime" % "ensime-sbt-cmd" % "0.0.7")
