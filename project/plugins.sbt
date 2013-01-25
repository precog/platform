resolvers ++= Seq(
  "sbt-idea-repo" at "http://mpeltonen.github.com/maven/",
  "Scala-Tools Maven2 Snapshots Repository" at "http://scala-tools.org/repo-snapshots")

resolvers += Resolver.url("artifactory",
  url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)


addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.8.5")

addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.2.0")

addSbtPlugin("org.ensime" % "ensime-sbt-cmd" % "0.1.0")

addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "2.1.0")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.7.0")
