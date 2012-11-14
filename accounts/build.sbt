name := "accounts"

libraryDependencies ++= Seq(
  "org.streum" % "configrity_2.9.1" % "0.9.0",
  "ch.qos.logback" % "logback-classic" % "1.0.0"
)
  
mainClass := Some("com.precog.accounts.MongoAccountServer")



