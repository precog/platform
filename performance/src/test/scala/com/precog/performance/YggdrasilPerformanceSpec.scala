package com.precog.performance

import org.specs2.mutable.Specification

import com.precog.common._
import com.precog.shard.yggdrasil._

import akka.dispatch.Await
import akka.util.Duration

import java.io.File

trait YggdrasilPerformanceSpec extends Specification with PerformanceSpec {
  "yggdrasil" should {
    val tmpFile = File.createTempFile("insert_test", "_db")
  
    step {    
      tmpFile.delete
      tmpFile.mkdirs
    }

    "insert 1M elements in 6s".performBatch(1000000, 8000) { i =>
      todo
    }

    "read 1M elements in 1s".performBatch(1000000, 1500) { i =>
      todo
    }
   
    // need cleanup here...
  }
}

