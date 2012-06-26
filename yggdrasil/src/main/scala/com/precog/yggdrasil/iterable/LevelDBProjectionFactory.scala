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
package iterable

import leveldb._
import com.precog.util.FileOps

import org.iq80.leveldb._
import org.fusesource.leveldbjni.JniDBFactory._
import org.fusesource.leveldbjni.DataWidth
import org.fusesource.leveldbjni.internal.JniDBIterator
import org.fusesource.leveldbjni.KeyValueChunk
import org.fusesource.leveldbjni.KeyValueChunk.KeyValuePair

import java.io.File
import java.util.concurrent.TimeoutException
import scala.collection.Iterator
import scalaz.NonEmptyList
import scalaz.Validation
import scalaz.effect._
import scalaz.syntax.validation._
import scalaz.iteratee.Input //todo: Get rid of!

trait LevelDBProjectionFactory extends ProjectionFactory[IterableDataset[Seq[CValue]]] {
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

  class ProjectionImpl private[LevelDBProjectionFactory] (val baseDir: File, val descriptor: ProjectionDescriptor) 
  extends LevelDBProjection with Projection[IterableDataset[Seq[CValue]]] {
    def traverseIndex(expiresAt: Long): IterableDataset[Seq[CValue]] = IterableDataset[Seq[CValue]](1, new Iterable[(Identities,Seq[CValue])]{
      def iterator = {
        val iter = idIndexFile.iterator.asInstanceOf[JniDBIterator]
        iter.seekToFirst

        val reader = new ChunkReader(iter, expiresAt)
        reader.start()

        new Iterator[(Identities,Seq[CValue])] {
          private[this] var currentChunk: Input[KeyValueChunk] = reader.chunkQueue.take()

          private final def nextIterator = 
            currentChunk.fold(
              empty = throw new TimeoutException("Iteration expired"),
              el    = chunk => chunk.getIterator(),
              eof   = emptyJavaIterator
            )

          private[this] var chunkIterator: java.util.Iterator[KeyValuePair] = nextIterator

          def hasNext: Boolean = if(currentChunk.isEof) false else {
            if (chunkIterator.hasNext) true else {
              currentChunk.foreach(chunk => reader.returnBuffers(chunk))
              currentChunk = reader.chunkQueue.take()
              chunkIterator = nextIterator
              chunkIterator.hasNext
            }
          }

          def next: (Identities,Seq[CValue]) = {
            val kvPair = chunkIterator.next()
            fromBytes(kvPair.getKey, kvPair.getValue)
          }
        }
      }
    })

    @inline 
    final def allRecords(expiresAt: Long): IterableDataset[Seq[CValue]] = traverseIndex(expiresAt)
  }
}


// vim: set ts=4 sw=4 et:
