package com.precog.performance

import org.specs2.mutable.Specification

import com.precog.yggdrasil.leveldb._

import org.iq80.leveldb._
import org.fusesource.leveldbjni.JniDBFactory._

import java.io.File
import java.nio.ByteBuffer

trait LeveldbPerformanceSpec extends Specification with PerformanceSpec {
  args(xonly = false)
  "leveldb" should {
    val tmpFile = File.createTempFile("insert_test", "_db")
  
    step {    
      tmpFile.delete
      tmpFile.mkdirs
    }

    "insert 1M elements in 4s".performBatch(1000000, 4000) { i =>
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

    "read 1M elements in 5s (naive)".performBatch(1000000, 5000) { i =>
      import org.fusesource.leveldbjni.internal.JniDBIterator
      val createOptions = (new Options).createIfMissing(true)  
      val db: DB = factory.open(tmpFile, createOptions)
      
      val chunkSize = 32 * 1024 
      
      val iter = db.iterator.asInstanceOf[JniDBIterator]
      iter.seekToFirst

      while(iter.hasNext) {
        val key = iter.peekNext.getKey
        val value = iter.peekNext.getValue
        iter.next
      }

      iter.close

      db.close
    }

    "read 1M elements in 750ms (batch)".performBatch(1000000, 750) { i =>
      import org.fusesource.leveldbjni.internal.JniDBIterator
      val createOptions = (new Options).createIfMissing(true)  
      val db: DB = factory.open(tmpFile, createOptions)
      
      val chunkSize = 32 * 1024 
      
      val iter = db.iterator.asInstanceOf[JniDBIterator]
      iter.seekToFirst

      while(iter.hasNext) {
        import org.fusesource.leveldbjni.KeyValueChunk
        val rawChunk: KeyValueChunk = iter.nextChunk(chunkSize)
        val actualChunkSize = rawChunk.getSize
        var el = 0
        while(el < actualChunkSize) {
          val key = rawChunk.keyAt(el)
          val value = rawChunk.valAt(el)
          el += 1
        }
      }

      iter.close

      db.close
    }
   
    // need cleanup here...
  }

}
