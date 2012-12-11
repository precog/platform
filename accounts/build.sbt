name := "accounts"

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.0.0"
)
  
mainClass := Some("com.precog.accounts.MongoAccountServer")



