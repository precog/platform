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
