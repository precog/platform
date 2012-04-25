package com.precog.yggdrasil
package iterable

import serialization._
import memoization._
import com.precog.common.VectorCase
import com.precog.util._

import akka.dispatch.Future

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

