package com.precog.performance

import org.specs2.mutable.Specification

import com.precog.yggdrasil.leveldb._

import org.iq80.leveldb._
import org.fusesource.leveldbjni.JniDBFactory._
import org.fusesource.leveldbjni.DataWidth

import java.io.File
import java.nio.ByteBuffer

trait LeveldbPerformanceSpec extends Specification with PerformanceSpec {
  args(xonly = false)
  "leveldb" should {
   
    sequential

    val tmpFile = File.createTempFile("insert_test", "_db")
  
    step {    
      tmpFile.delete
      tmpFile.mkdirs
    }

    "insert 1M elements in 3.5s".performBatch(1000000, 3500) { i =>
      val createOptions = (new Options).createIfMissing(true)  
      val db: DB = factory.open(tmpFile, createOptions)

      val key = new Array[Byte](8)
      val value = new Array[Byte](8)

      val keyBuf = ByteBuffer.wrap(key)
      val valueBuf = ByteBuffer.wrap(value)

      var cnt = 0

      while(cnt < i) {
        keyBuf.clear
        valueBuf.clear

        keyBuf.put(cnt)
        valueBuf.put(i-cnt)

        db.put(key, value)
        cnt += 1
      }

      db.close
    }

    "read 1M elements in 2.5s (naive)".performBatch(1000000, 2500) { i =>
      import org.fusesource.leveldbjni.internal.JniDBIterator
      val createOptions = (new Options).createIfMissing(true)  
      val db: DB = factory.open(tmpFile, createOptions)
      
      val iter = db.iterator.asInstanceOf[JniDBIterator]
      iter.seekToFirst

      while(iter.hasNext) {
        val map = iter.next
        val key = map.getKey
        val value = map.getValue
      }

      iter.close

      db.close
    }

    "read 1M elements in 650ms (batch)".performBatch(1000000, 650) { i =>
      import org.fusesource.leveldbjni.internal.JniDBIterator
      val createOptions = (new Options).createIfMissing(true)  
      val db: DB = factory.open(tmpFile, createOptions)
      
      val chunkSize = 32 * 1024 
      
      val iter = db.iterator.asInstanceOf[JniDBIterator]
      iter.seekToFirst

      val chunkBuffer = ByteBuffer.allocate(chunkSize)

      while(iter.hasNext) {
        val chunkItr = iter.nextChunk(chunkBuffer, DataWidth.VARIABLE, DataWidth.VARIABLE).getIterator
        while(chunkItr.hasNext) {
          val kvPair = chunkItr.next()
          val key = kvPair.getKey
          val value = kvPair.getValue 
        }
      }

      iter.close

      db.close
    }
   
    // need cleanup here...
  }

}
