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
package com.precog.benchmarking

object Benchmark extends App {
  args match {
    case Array(warmups, runs, count) => {
//      val benches = List(JDBMBench,LevelDBBench)
      val benches = List(JDBM3Bench)
      val times: List[(Seq[Long],Seq[Long])] = benches.map {
        bench => bench.run(warmups.toInt, runs.toInt, count.toLong)
      }

      val columns = times.map {
        case (a,b) => (a zip b) map {
          case (x,y) => List(x, y)
        }
      }.reduceLeft {
        (l1: Seq[List[Long]],l2: Seq[List[Long]]) => (l1 zip l2).map {
          case (la,lb) => la ::: lb 
        }
      }

      println(benches.map { b => b.name + " write\t" + b.name + " read" }.mkString("Run\t", "\t", ""))
      
      columns.zipWithIndex.foreach { case (t,idx) => println((idx + 1) + "\t" + t.mkString("\t")) }
    }
    case _ => println("Usage: Benchmark <warmup count> <run count> <element count per run>")
  }
}
