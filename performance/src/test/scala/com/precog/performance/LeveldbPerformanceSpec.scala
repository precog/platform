package com.precog.performance

import org.specs2.mutable._

import com.precog.yggdrasil.leveldb._

import org.iq80.leveldb._
import org.fusesource.leveldbjni.JniDBFactory._
import org.fusesource.leveldbjni.DataWidth

import java.io.File
import java.nio.ByteBuffer

import com.weiglewilczek.slf4s.Logging

trait LeveldbPerformanceSpec extends Specification with PerformanceSpec with Logging {

  "leveldb" should {
    sequential
   
    val tmpDir = newTempDir 
  
    "insert" in {
      val createOptions = (new Options).createIfMissing(true)  
      val db: DB = factory.open(tmpDir, createOptions)
      try { 
        performBatch(1000000, 8000) { i =>

          val key = new Array[Byte](8)
          val value = new Array[Byte](8)

          val keyBuf = ByteBuffer.wrap(key)
          val valueBuf = ByteBuffer.wrap(value)

          var cnt = 0

          while(cnt < i) {
            keyBuf.clear
            valueBuf.clear

            keyBuf.putLong(cnt)
            valueBuf.putLong(i-cnt)

            db.put(key, value)
            cnt += 1
          }
        }
      } finally {
        db.close
      }
    }

    "read (naive)" in {
      import org.fusesource.leveldbjni.internal.JniDBIterator
      val createOptions = (new Options).createIfMissing(true)  
      val db: DB = factory.open(tmpDir, createOptions)
      try { 
        performBatch(1000000, 4500) { i =>
          val iter = db.iterator.asInstanceOf[JniDBIterator]
          iter.seekToFirst

          while(iter.hasNext) {
            val map = iter.next
            val key = map.getKey
            val value = map.getValue
          }

          iter.close
        }
      } finally {
        db.close
      }
    }

    "read batch" in {
      import org.fusesource.leveldbjni.internal.JniDBIterator
      val createOptions = (new Options).createIfMissing(true)  
      val db: DB = factory.open(tmpDir, createOptions)
     
      try {
        performBatch(1000000, 1500) { i =>
        
          val chunkSize = 32 * 1024 
          
          val iter = db.iterator.asInstanceOf[JniDBIterator]
          iter.seekToFirst

          val keyBuffer = ByteBuffer.allocate(chunkSize)
          val valBuffer = ByteBuffer.allocate(chunkSize)

          while(iter.hasNext) {
            val chunkItr = iter.nextChunk(keyBuffer, valBuffer, DataWidth.VARIABLE, DataWidth.VARIABLE).getIterator
            while(chunkItr.hasNext) {
              val kvPair = chunkItr.next()
              val key = kvPair.getKey
              val value = kvPair.getValue 
            }
          }

          iter.close
        }
      } finally {
        db.close
      }
    }

    "read optimal" in {
      import org.fusesource.leveldbjni.internal.JniDBIterator
      val createOptions = (new Options).createIfMissing(true).blockSize(64 * 1024)
      val db: DB = factory.open(tmpDir, createOptions)
     
      val chunkSize = 64 * 1024

      val keyBuffer = ByteBuffer.allocate(chunkSize)
      val valBuffer = ByteBuffer.allocate(chunkSize)

      try {
        performBatch(1000000, 1000) { i =>  
          val iter = db.iterator.asInstanceOf[JniDBIterator]
          iter.seekToFirst

          while(iter.hasNext) {
            val start = System.currentTimeMillis
            var count = iter.nextChunk(keyBuffer, valBuffer, DataWidth.FIXED(8), DataWidth.FIXED(8)).getSize
            val end = System.currentTimeMillis

            logger.trace("Chunk retrieved %d values in %dms".format(count, (end - start)))

            while(count > 0) {
              val key = keyBuffer.getLong
              val value = valBuffer.getLong

              count -= 1
            }
          }

          iter.close
        }
      } finally {
        db.close
      }
    }

    "cleanup" in {
      cleanupTempDir(tmpDir)
      success
    }
  }
}
