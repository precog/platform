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

import com.precog.yggdrasil.leveldb._

import org.iq80.leveldb._
import org.fusesource.leveldbjni.JniDBFactory._

import java.io.File

import blueeyes.json.JsonParser
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._

object YggUtils {

  import JsonParser._
  
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
    colDirs foreach { columnSummary }
  }

  def isDotDir(f: File) = f.isDirectory && (f.getName == "." || f.getName == "..")

  def columnSummary(colDir: File) {
    val createOptions = (new Options).createIfMissing(false)

    val rawDescriptor = scala.io.Source.fromFile(new File(colDir, "projection_descriptor.json")).mkString
    val descriptor = parse(rawDescriptor).validated[ProjectionDescriptor].toOption.get

    val db: DB = factory.open(new File(colDir, "idIndex"), createOptions.comparator(LevelDBProjectionComparator(descriptor)))
    try {
      val iterator: DBIterator = db.iterator
      println
      println("Stats for column: " + colDir.getName)
      try {
        iterator.seekToFirst
        var cnt = 0
        var keyLengthSum = 0.0
        var valueLengthSum = 0.0
        while(iterator.hasNext) {
          val key: Array[Byte] = iterator.peekNext().getKey
          val value: Array[Byte] = iterator.peekNext().getValue
          cnt += 1
          keyLengthSum += key.length
          valueLengthSum += value.length
          iterator.next
        }

        println("  rows: %d avg-key-len: %.01f avg-val-len: %.01f".format(cnt, keyLengthSum / cnt, valueLengthSum / cnt)) 
        
      } finally {
        iterator.close();
      }
    } finally {
      db.close 
    }
  }

  def columnDetail(dirname: String) { columnSummary(new File(dirname)) }

}
