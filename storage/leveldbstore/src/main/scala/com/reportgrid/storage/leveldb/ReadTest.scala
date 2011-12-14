package com.reportgrid.storage.leveldb

object ReadTest {
  def main (argv : Array[String]) {
    val (column, chunkSize, seed) = argv match {
      case Array(name, basedir, size, sd) => (new Column[Long](name,basedir), size.toInt, sd.toInt)
      case _ => {
        println("Usage: ReadTest <column name> <base dir>")
        sys.exit(1)
      }
    }

    // Setup our PRNG
    val r = new java.util.Random(seed)

    // Get a set of all values in the column
    val allVals = column.getAllValues.toArray

    println("Read " + allVals.size + " distinct values")

    // Outer loop forever
    while (true) {
      var index = 0
      var totalRead = 0
      val startTime = System.currentTimeMillis
      while (index < chunkSize) {
        val toRead = allVals(r.nextInt(allVals.size))
        val relatedIds = column.getIds(toRead)
        totalRead += relatedIds.size
        println(toRead + " => " + relatedIds)
        index += 1
      }
      val duration = System.currentTimeMillis - startTime

      println("%d\t%f".format(totalRead, chunkSize / (duration / 1000.0)))
    }
  }
}

object ConfirmTest {
  def main (args : Array[String]) {
    args match {
      case Array(name,base) => {
        val c = new Column[java.math.BigDecimal](name, base)
        val bd = BigDecimal("123.45")

        List(12l,15l,45345435l,2423423l).foreach(c.insert(_, bd.underlying))

        println(c.getIds(bd.underlying).toList)
      }
    }
  }
}
