/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.yggdrasil
package util

import com.precog.common.Path
import com.precog.common.util._
import com.precog.yggdrasil.leveldb._

import org.iq80.leveldb._
import org.fusesource.leveldbjni.JniDBFactory._

import java.io.File
import java.nio.ByteBuffer

import blueeyes.json.JPath
import blueeyes.json.JsonParser
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._

import scopt.mutable.OptionParser

import scala.collection.SortedSet
import scala.collection.SortedMap

object YggUtils {

  import JsonParser._
  
  case class ColumnSummary(types: Seq[CType], count: Long, keyLengthSum: Long, valueLengthSum: Long) {
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

  val commands: Map[String, Command] = List(
    DescriptorSummary 
  ).map( c => (c.name, c) )(collection.breakOut)

  def main(args: Array[String]) {
    if(args.length > 0) {
      commands.get(args(0)).map { c =>
        c.run(args.slice(1,args.length))
      }.getOrElse {
        run(args(0))
      }
    } else {
      die(usage)
    }
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
    val rawDescriptor = IOUtils.rawReadFileToString(new File(colDir, "projection_descriptor.json"))
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

  def columnStats(colDir: File, colType: CType, desc: ProjectionDescriptor): ColumnSummary = {
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

trait Command {
  def name(): String
  def description(): String
  def run(args: Array[String])
}

object DescriptorSummary extends Command {
  val name = "describe" 
  val description = "describe paths and selectors" 
  
  def run(args: Array[String]) {
    println(args.mkString(","))
    val config = new Config
    val parser = new OptionParser("ygg describe") {
      opt("p", "path", "<path>", "root data path", {p: String => config.path = Some(Path(p))})
      opt("s", "selector", "<selector>", "root object selector", {s: String => config.selector = Some(JPath(s))})
      booleanOpt("v", "verbose", "<vebose>", "show selectors as well", {v: Boolean => config.verbose = v})
      arg("<datadir>", "shard data dir", {d: String => config.dataDir = d})
    }
    if (parser.parse(args)) {
      process(config)
    } else { 
      parser
    }
  }

  implicit val tOrdering = new Ordering[(JPath, CType)] {
    def compare(a: (JPath, CType), b: (JPath, CType)): Int = {
      val jord = implicitly[Ordering[JPath]]
      val sord = implicitly[Ordering[String]] 
      val jo = jord.compare(a._1, b._1)
      if(jo != 0) {
        jo 
      } else {
        sord.compare(a._2.toString, b._2.toString)
      }
    }
  }

  def process(config: Config) {
    println("describing data at %s matching %s%s".format(config.dataDir, 
                                                         config.path.map(_.toString).getOrElse("*"),
                                                         config.selector.map(_.toString).getOrElse(".*")))
    
    show(extract(load(config.dataDir)), config.verbose)
  }

  def load(dataDir: String) = {
    val dir = new File(dataDir)
    for(d <- dir.listFiles if d.isDirectory && !(d.getName == "." || d.getName == "..")) yield {
      readDescriptor(d)
    }
  }

  def extract(descs: Array[ProjectionDescriptor]): SortedMap[Path, SortedSet[(JPath, CType)]] = {
    implicit val pord = new Ordering[Path] {
      val sord = implicitly[Ordering[String]] 
      def compare(a: Path, b: Path) = sord.compare(a.toString, b.toString)
    }
    descs.foldLeft(SortedMap[Path,SortedSet[(JPath, CType)]]()) {
      case (acc, desc) =>
       desc.columns.foldLeft(acc) {
         case (acc, ColumnDescriptor(p, s, t, _)) =>
           val update = acc.get(p) map { _ + (s -> t) } getOrElse { SortedSet((s, t)) } 
           acc + (p -> update) 
       }
    }
  }

  def show(summary: SortedMap[Path, SortedSet[(JPath, CType)]], verbose: Boolean) {
    summary.foreach { 
      case (p, sels) =>
        println(p)
        if(verbose) {
          sels.foreach {
            case (s, ct) => println("  %s -> %s".format(s, ct))
          }
          println
        }
    }
  }

  def readDescriptor(dir: File): ProjectionDescriptor = {
    val rawDescriptor = IOUtils.rawReadFileToString(new File(dir, "projection_descriptor.json"))
    JsonParser.parse(rawDescriptor).validated[ProjectionDescriptor].toOption.get
  }

  class Config(var path: Option[Path] = None, 
               var selector: Option[JPath] = None,
               var dataDir: String = ".",
               var verbose: Boolean = false)
}
