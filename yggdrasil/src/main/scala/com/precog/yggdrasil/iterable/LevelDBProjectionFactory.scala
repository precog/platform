package com.precog.yggdrasil
package iterable

import leveldb._
import com.precog.util.FileOps

import org.iq80.leveldb._
import org.fusesource.leveldbjni.JniDBFactory._
import org.fusesource.leveldbjni.DataWidth
import org.fusesource.leveldbjni.internal.JniDBIterator
import org.fusesource.leveldbjni.KeyValueChunk
import org.fusesource.leveldbjni.KeyValueChunk.KeyValuePair
import org.joda.time.DateTime

import java.io.File
import java.nio.ByteBuffer
import java.util.concurrent.{Executors,TimeoutException}

import scala.collection.Iterator
import scalaz.NonEmptyList
import scalaz.Validation
import scalaz.effect._
import scalaz.syntax.validation._
import scalaz.iteratee.Input //todo: Get rid of!

trait LevelDBProjectionsModule extends ProjectionsModule {
  // pool for readahead threads
  private val readaheadPool = Executors.newCachedThreadPool()

  class ProjectionImpl private[LevelDBProjectionsModule] (baseDir: File, descriptor: ProjectionDescriptor) 
  extends LevelDBProjection(baseDir, descriptor) with FullProjection[IterableDataset[Seq[CValue]]] {
    ///////////////////
    // ID Traversals //
    ///////////////////

    val readAheadSize = 2 // TODO: Make configurable
    val readPollTime = 100l

    import java.util.concurrent.{ArrayBlockingQueue,TimeUnit}
    import java.util.concurrent.atomic.AtomicBoolean
    import org.fusesource.leveldbjni.internal.JniDBIterator
    import org.fusesource.leveldbjni.KeyValueChunk
    import org.fusesource.leveldbjni.KeyValueChunk.KeyValuePair

    sealed trait ChunkReadResult
    case class ChunkData(c: KeyValueChunk) extends ChunkReadResult
    case object ChunkEOF extends ChunkReadResult
    case class ChunkTimeout(epochDate: Long) extends ChunkReadResult

    class ChunkReader(iterator: JniDBIterator, expiresAt: Long) extends Runnable {
      val bufferQueue = new ArrayBlockingQueue[Pair[ByteBuffer,ByteBuffer]](readAheadSize) // Need a key and value buffer for each readahead

      // pre-fill the buffer queue
      (1 to readAheadSize).foreach {
        _ => bufferQueue.put((ByteBuffer.allocate(chunkSize), ByteBuffer.allocate(chunkSize)))
      }

      def returnBuffers(chunk: KeyValueChunk) {
        bufferQueue.put((chunk.keyData, chunk.valueData))
      }
      
      val chunkQueue  = new ArrayBlockingQueue[ChunkReadResult](readAheadSize + 1)

      override def run() {
        if (iterator.hasNext) {
          var buffers : Pair[ByteBuffer,ByteBuffer] = null
          while (buffers == null) {
            if (System.currentTimeMillis > expiresAt) {
              iterator.close()
              chunkQueue.put(ChunkTimeout(System.currentTimeMillis))
              return
            }
            buffers = bufferQueue.poll(readPollTime, TimeUnit.MILLISECONDS)
          } 

          if (buffers != null) {
            val chunk = ChunkData(iterator.nextChunk(buffers._1, buffers._2, DataWidth.VARIABLE, DataWidth.VARIABLE))

            while (! chunkQueue.offer(chunk, readPollTime, TimeUnit.MILLISECONDS)) {
              if (System.currentTimeMillis > expiresAt) {
                iterator.close()
                chunkQueue.put(ChunkTimeout(System.currentTimeMillis))
                return
              }
            }
          }

          // We didn't expire, so reschedule
          readaheadPool.execute(this)
        } else {
          chunkQueue.put(ChunkEOF) // We're here because we reached the end of the iterator, so block and submit
          iterator.close()
        }
      }
    }

    def traverseIndex(expiresAt: Long): IterableDataset[Seq[CValue]] = IterableDataset[Seq[CValue]](1, new Iterable[(Identities,Seq[CValue])]{
      def iterator = {
        val iter = idIndexFile.iterator.asInstanceOf[JniDBIterator]
        iter.seekToFirst

        val reader = new ChunkReader(iter, expiresAt)
        readaheadPool.execute(reader)
        
        new Iterator[(Identities,Seq[CValue])] {
          private[this] var currentChunk: Option[KeyValueChunk] = None
          private[this] var chunkIterator: java.util.Iterator[KeyValuePair] = nextIterator()

          private[this] def nextIterator() = {
            currentChunk.foreach(reader.returnBuffers)
            reader.chunkQueue.take() match {
              case ChunkData(data)  => currentChunk = Some(data); data.getIterator()
              case ChunkEOF         => null
              case ChunkTimeout(at) => throw new TimeoutException("Iteration expired at " + new DateTime(at))
            }
          }

          private[this] def computeNext() : KeyValuePair = {
            if (chunkIterator == null) {
              null
            } else if (chunkIterator.hasNext) {
              chunkIterator.next()
            } else {
              chunkIterator = nextIterator()
              computeNext()
            } 
          }

          private[this] var next0 = computeNext()

          def hasNext: Boolean = next0 != null

          def next: (Identities,Seq[CValue]) = {
            val current = next0
            next0 = computeNext()
            fromBytes(current.getKey, current.getValue)
          }
        }
      }
    })

    @inline final def allRecords(expiresAt: Long): IterableDataset[Seq[CValue]] = traverseIndex(expiresAt)
  }  
  
  
  trait LevelDBProjectionFactory extends ProjectionFactory {
    def fileOps: FileOps

    def baseDir(descriptor: ProjectionDescriptor): File

    def projection(descriptor: ProjectionDescriptor): IO[ProjectionImpl] = {
      val base = baseDir(descriptor)
      val baseDirV: IO[File] = 
        fileOps.exists(base) flatMap { 
          case true  => IO(base)
          case false => fileOps.mkdir(base) map {
                          case true  => base
                          case false => throw new RuntimeException("Could not create database basedir " + base)
                        }
        }

      baseDirV map { (bd: File) => new ProjectionImpl(bd, descriptor) }
    }

    def close(projection: ProjectionImpl) = IO(projection.close())
  }
}


// vim: set ts=4 sw=4 et:
