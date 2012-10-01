dataDir := {
  new java.io.File("jprofiler/jprofiler.db").getCanonicalPath
}

extractData <<= (dataDir, streams) map { (dir, s) =>
  val d = new File(dir)
  val data = new File(d, "data")
  val path = data.getCanonicalPath
  if (d.exists) {
    s.log.info("Deleting old data from %s" format path)
    sbt.IO.delete(d)
  }
  if (!data.mkdirs())
    throw new Exception("Failed to create %s" format path)
  try {
    s.log.info("Extracting data into %s" format path)
    val args = Seq(path, System.getProperty("java.class.path"))
    Process("./regen-jdbm-data.sh", args).!!
  } catch {
    case t: Throwable => s.log.error("Failed to extract to %s" format path)
  }
  d.getCanonicalPath
}
