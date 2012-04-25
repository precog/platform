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
package com.precog
package daze

import akka.dispatch.Future
import yggdrasil._
import yggdrasil.serialization._
import memoization._
import com.precog.common.VectorCase
import com.precog.util._

import scala.annotation.tailrec
import scalaz._
import scalaz.std.anyVal.longInstance

trait Buffering[A] {
  implicit val order: Order[A]
  def newBuffer(size: Int): Buffer

  trait Buffer {
    val buffer: Array[A]
    def sort(limit: Int): Unit
    def bufferChunk(v: Iterator[A]): Int = {
      @tailrec def insert(j: Int, iter: Iterator[A]): Int = {
        if (j < buffer.length && iter.hasNext) {
          buffer(j) = iter.next()
          insert(j + 1, iter)
        } else j
      }

      insert(0, v)
    }
  }
}

object Buffering {
  import java.util.Arrays

  implicit def refBuffering[A <: AnyRef](implicit ord: Order[A], cm: ClassManifest[A]): Buffering[A] = new Buffering[A] {
    implicit val order = ord
    private val javaOrder = order.toJavaComparator

    def newBuffer(size: Int) = new Buffer {
      val buffer = new Array[A](size)
      def sort(limit: Int) = Arrays.sort(buffer, 0, limit, javaOrder)
    }
  }

  implicit object longBuffering extends Buffering[Long] {
    implicit val order = longInstance
    def newBuffer(size: Int) = new Buffer {
      val buffer = new Array[Long](size)
      def sort(limit: Int) = Arrays.sort(buffer, 0, limit)
    }
  }
}

