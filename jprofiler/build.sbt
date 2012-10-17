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
  def performExtract = {
    s.log.info("Extracting data into %s" format path)
    if (Process("./regen-jdbm-data.sh", Seq(path)).! != 0) {
      error("Failed to extract to %s" format path)
    } else {
      s.log.info("Extraction complete.")
      d.getCanonicalPath
    }
  }  
  if (!data.isDirectory()) {
    if (!data.mkdirs()) {
      error("Failed to create %s")
    } else {
      performExtract
    }
  } else {
    if (data.listFiles.length > 0) {
      s.log.info("Using data in %s" format path)
      d.getCanonicalPath  
    } else {
      performExtract
    }
  }
}
