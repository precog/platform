package com.precog.yggdrasil
package util

import com.precog.yggdrasil.leveldb._

import org.iq80.leveldb._
import org.fusesource.leveldbjni.JniDBFactory._

import java.io.File
import java.nio.ByteBuffer

import blueeyes.json.JPath
import blueeyes.json.JsonParser
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._

import com.precog.analytics.Path

object YggUtils {

  import JsonParser._
  
  case class ColumnSummary(types: Seq[ColumnType], count: Long, keyLengthSum: Long, valueLengthSum: Long) {
    def +(other: ColumnSummary) = ColumnSummary(types ++ other.types, 
                                            count + other.count, 
                                            keyLengthSum + other.keyLengthSum, 
                                            valueLengthSum + other.valueLengthSum)

    def meanKeyLength: Double = keyLengthSum.toDouble / count
    def meanValueLength: Double = valueLengthSum.toDouble / count
  }
  
  val usage = """
Usage: command {dbRoot|colRoot}

dbRoot - path to database root (will show a summary of all columns in the database)
colRoot - path to a specific column root (will show a more detailed view of a specific column)
 """

  def main(args: Array[String]) {
    if(args.length == 0) die(usage) else run(args(0)) 
  }

  def run(dirname: String) {
    dirname match {
      case d if isDBRoot(d)     =>
        databaseSummary(d)
      case d if isColumnRoot(d) => 
        columnDetail(d)
      case d                    => 
        die("The given directory is neither the database root or a column root. [%s]".format(d))
    }
  }

  def die(msg: String, code: Int = 1) {
    println(msg)
    sys.exit(code)
  }

  def isDBRoot(dirname: String) = new File(dirname, "checkpoints.json").canRead

  def isColumnRoot(dirname: String) = new File(dirname, "projection_descriptor.json").canRead

  def databaseSummary(dirname: String) {
    val f = new File(dirname)
    val colDirs = for(colDir <- f.listFiles if colDir.isDirectory && !isDotDir(colDir)) yield { colDir }
    val summary = colDirs.foldLeft(Map[Path, Map[JPath, ColumnSummary]]()){
      case (acc, colDir) => columnSummary(colDir, acc) 
    }

    printSummary(summary)
  }

  def printSummary(summary: Map[Path, Map[JPath, ColumnSummary]]) {
    println("----------------")
    println("Database Summary")
    println("----------------")

    summary.toList.sortBy( _._1.toString ) foreach {
      case (path, selectors) =>
        println
        println("%s".format(path.toString))
        selectors.toList.sortBy( _._1.toString ) foreach {
          case (selector, summary) => println("  %-15s %-20s %10d (%.02f/%.02f)".format(selector.toString, summary.types.mkString(","), summary.count, summary.meanKeyLength, summary.meanValueLength))
        }
    }

    println()
  }

  def isDotDir(f: File) = f.isDirectory && (f.getName == "." || f.getName == "..")


  def columnSummary(colDir: File, acc: Map[Path, Map[JPath, ColumnSummary]]): Map[Path, Map[JPath, ColumnSummary]] = {
    val rawDescriptor = scala.io.Source.fromFile(new File(colDir, "projection_descriptor.json")).mkString
    val descriptor = parse(rawDescriptor).validated[ProjectionDescriptor].toOption.get

    descriptor.columns.foldLeft( acc ) { 
      case (acc, ColumnDescriptor(path, sel, colType, _)) =>
        val colSummary = columnStats(colDir, colType, descriptor)
        acc.get(path) map {
          case pathMap => pathMap.get(sel) map { summary =>
              acc + (path -> (pathMap + (sel -> (summary + colSummary))))
            } getOrElse {
              acc + (path -> (pathMap + (sel -> colSummary))) 
            }
        } getOrElse {
          acc + (path -> (Map[JPath, ColumnSummary]() + (sel -> colSummary)))
        }
    }
  }

  def columnStats(colDir: File, colType: ColumnType, desc: ProjectionDescriptor): ColumnSummary = {
    val createOptions = (new Options).createIfMissing(false)

    val db: DB = factory.open(new File(colDir, "idIndex"), createOptions.comparator(LevelDBProjectionComparator(desc)))
    try {
      val iterator: DBIterator = db.iterator
      try {
        iterator.seekToFirst
        var cnt = 0
        var keyLengthSum = 0
        var valueLengthSum = 0
        while(iterator.hasNext) {
          val key: Array[Byte] = iterator.peekNext().getKey
          val value: Array[Byte] = iterator.peekNext().getValue
          cnt += 1
          keyLengthSum += key.length
          valueLengthSum += value.length
          iterator.next
        }

        ColumnSummary(List(colType), cnt, keyLengthSum, valueLengthSum) 
      } finally {
        iterator.close();
      }
    } finally {
      db.close 
    }
  }

  def columnDetail(dirname: String) { 
    println("Column detail not yet implemented.")
  }

}

object LevelDBOpenFileTest extends App {
    val createOptions = (new Options).createIfMissing(true)
    val db: DB = factory.open(new File("./test"),createOptions) 

    var cnt = 0L
    var payload = Array[Byte](12)

    val key = new Array[Byte](8)
    val buf = ByteBuffer.wrap(key)

    while(true) {
      val batch = db.createWriteBatch()
      try {
        var batchCnt = 0
        val limit = 100
        while(batchCnt < limit) {
          buf.clear
          buf.putLong(cnt)
          batch.put(key, payload)
          batchCnt += 1
          cnt += 1
        }

        db.write(batch)
      } finally {
        // Make sure you close the batch to avoid resource leaks.
        batch.close()
      }
      if(cnt % 1000 == 0) println("Insert count: " + cnt) 
    }
}
