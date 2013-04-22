name := "accounts"

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.0.0",
  "org.jvnet.mock-javamail" % "mock-javamail" % "1.9" % "test"
)

mainClass := Some("com.precog.accounts.MongoAccountServer")
