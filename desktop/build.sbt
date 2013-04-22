import sbtassembly.Plugin._
import AssemblyKeys._

// Workaround for dependency breakage due to zkclient in kafka-core
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("org", "apache", "zookeeper", xs @ _*) => MergeStrategy.first
    case PathList("org", "apache", "jute", xs @ _*) => MergeStrategy.first
    case PathList("META-INF", xs @ _*) =>
      (xs map {_.toLowerCase}) match {
        case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
          MergeStrategy.discard
        case _ => MergeStrategy.first
      }
    case _ => MergeStrategy.first
  }
}
