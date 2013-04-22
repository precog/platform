name := "util"

libraryDependencies ++= Seq(
  "commons-io" %  "commons-io" % "2.3",
  "javax.mail" % "mail"        % "1.4",
  "org.fusesource.scalate" % "scalate-core_2.9" % "1.6.1"
)

logBuffered := false // gives us incremental output from Specs2
