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
package com.precog.shard

import java.nio.CharBuffer
import java.lang.Character.isWhitespace

import scalaz._
import scalaz.syntax.monad._

object TerminateJson {
  final class ArrayStack[@specialized(Int) A: Manifest](initCapacity: Int = 128) {
    private[shard] var stack = new Array[A](initCapacity)
    private[shard] var next: Int = 0

    private def resize() {
      val stack0 = new Array[A](stack.length * 2)
      System.arraycopy(stack, 0, stack0, 0, stack.length)
      stack = stack0
    }

    def copy(): ArrayStack[A] = {
      val copy = new ArrayStack[A](stack.length)
      System.arraycopy(stack, 0, copy.stack, 0, next)
      copy.next = next
      copy
    }

    def head: A = {
      if (next == 0)
        throw new IllegalStateException("Cannot pop element off of empty stack.")
      stack(next - 1)
    }

    def push(a: A) {
      if (next == stack.length) resize()
      stack(next) = a
      next += 1
    }

    def pop(): A = {
      if (next == 0)
        throw new IllegalStateException("Cannot pop element off of empty stack.")
      next -= 1
      stack(next)
    }

    def isEmpty: Boolean = next == 0
    def nonEmpty: Boolean = next > 0
    override def toString: String =
      stack.take(next).mkString("ArrayStack(", ", ", ")")
  }

  @inline private final val ExpectValue = 0
  @inline private final val ExpectField = 1
  @inline private final val SkipChar = 2
  @inline private final val FieldDelim = 3
  @inline private final val CloseString = 4
  @inline private final val CloseArray = 5
  @inline private final val CloseObject = 6

  /**
   * Ensures the successful termination of a stream of JSON. This assumes quite
   * a bit. First, aside from the fact that it may end abruptly, it assumes the
   * JSON is valid. Using this on invalid JSON will produce unexpected results.
   * Next, it assumes that all numeric, null and boolean values are produced in
   * their entirety or not at all. Lastly, it assumes that there should be at
   * least 1 value in the stream. If the stream is empty, then `null` will be
   * inserted.
   */
  def ensure[M[+_]: Monad](stream0: StreamT[M, CharBuffer]): StreamT[M, CharBuffer] = {
    def build(stack0: ArrayStack[Int], buf: CharBuffer): (ArrayStack[Int], CharBuffer) = {
      val stack = stack0.copy()

      while (buf.remaining() > 0) {
        val c = buf.get()

        def anyValue(): Boolean = c match {
          case '"' => stack.push(CloseString); true
          case '[' => stack.push(CloseArray); true
          case '{' => stack.push(CloseObject); true
          case s if isWhitespace(s) => false
          case c => true
        }

        if (stack.isEmpty) {
          anyValue()
        } else {
          stack.head match {
            case SkipChar =>
              stack.pop()
            case ExpectValue =>
              if (!isWhitespace(c)) stack.pop()
              anyValue()
            case CloseString =>
              if (c == '\\') stack.push(SkipChar)
              else if (c == '"') stack.pop()
            case CloseArray =>
              if (c == ']') stack.pop()
              else if (c == ',') stack.push(ExpectValue)
              else anyValue()
            case ExpectField =>
              if (c == '"') {
                stack.pop()
                stack.push(ExpectValue)
                stack.push(FieldDelim)
                stack.push(CloseString)
              }
            case CloseObject =>
              if (c == '}') stack.pop()
              else if (c == ',') stack.push(ExpectField)
              else if (c == '"') {
                stack.push(ExpectValue)
                stack.push(FieldDelim)
                stack.push(CloseString)
              } else anyValue()
            case FieldDelim =>
              if (c == ':') stack.pop()
            case _ =>
              anyValue()
          }
        }
      }

      buf.flip()
      (stack, buf)
    }

    def terminal(stack: ArrayStack[Int]): Option[CharBuffer] = {
      if (stack.isEmpty) None else Some({
        val sb = new StringBuilder()
        while (stack.nonEmpty) {
          sb ++= (stack.pop() match {
            case ExpectValue => "null"
            case ExpectField => "\"\":null"
            case SkipChar => "\""
            case FieldDelim => ":"
            case CloseString => "\""
            case CloseArray => "]"
            case CloseObject => "}"
            case _ => sys.error("Unreachable.")
          })
        }
        CharBuffer.wrap(sb.toString)
      })
    }

    def rec(stack0: ArrayStack[Int], stream: StreamT[M, CharBuffer]): StreamT[M, CharBuffer] = {
      StreamT(stream.uncons map {
        case Some((buf0, tail)) =>
          val (stack, buf) = build(stack0, buf0)
          StreamT.Yield(buf, rec(stack, tail))
        case None =>
          terminal(stack0) map { buf =>
            StreamT.Yield(buf, StreamT.empty)
          } getOrElse StreamT.Done
      })
    }

    val init = new ArrayStack[Int]
    init.push(ExpectValue)
    rec(init, stream0)
  }
}


