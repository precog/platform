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

import annotation.tailrec
import collection.immutable.Stack

import java.nio.CharBuffer

object RecoverJson {

  sealed trait Balanced
  case class Colon(v: Balanced) extends Balanced
  case class Key(v: Colon) extends Balanced
  case object Brace extends Balanced
  case object Bracket extends Balanced
  case object NullValue extends Balanced
  case object Quote extends Balanced
  case object EscapeChar extends Balanced

  @tailrec private def findEndString(buffers: Vector[CharBuffer], bufferIndex: Int, offset: Int): Option[(Int, Int)] = {
    if (bufferIndex >= buffers.length)
      None
    else if (offset >= buffers(bufferIndex).limit)
      findEndString(buffers, bufferIndex + 1, offset % buffers(bufferIndex).limit)
    else {
      val char = buffers(bufferIndex).get(offset)

      if (char == '"')
        Some((bufferIndex, offset))
      else if (char == '\\')
        findEndString(buffers, bufferIndex, offset + 2)
      else
        findEndString(buffers, bufferIndex, offset + 1)
    }
  }

  private case class BalancedStackState(bufferIndex: Int, offset: Int, stack: Stack[Balanced]) {
    def increment(balanced: Balanced) = BalancedStackState(
      bufferIndex,
      offset + 1,
      stack push balanced
    )
    def decrement = BalancedStackState(
      bufferIndex,
      offset + 1,
      stack pop
    )
    def skip = BalancedStackState(
      bufferIndex,
      offset + 1,
      stack
    )
  }

  private def balancedStack(buffers: Vector[CharBuffer]) = {
    @tailrec @inline def buildState(accum: BalancedStackState): BalancedStackState =
      if (accum.bufferIndex >= buffers.length)
        accum
      else if (accum.offset >= buffers(accum.bufferIndex).limit)
        buildState(BalancedStackState(
          accum.bufferIndex + 1,
          accum.offset % buffers(accum.bufferIndex).limit,
          accum.stack
        ))
      else {
        val buffer = buffers(accum.bufferIndex)
        var char = buffer.get(accum.offset)
        char match {
          case '{' =>
            val next = accum increment Brace
            buildState(next.copy(stack = next.stack push Colon(NullValue)))
          case '[' =>
            buildState(accum increment Bracket)

          case '}' =>
            // Assumption: will never output valid {}
            buildState(accum decrement)
          case ']' =>
            buildState(accum decrement)

          case ',' if accum.stack.nonEmpty && accum.stack.head == Brace =>
            buildState(accum increment Key(Colon(NullValue)))
          case ':' =>
            val next = accum.decrement
            buildState(next.copy(stack = next.stack push NullValue))

          case '"' =>
            val next = accum.stack.headOption match {
              case Some(NullValue) => accum.decrement
              case Some(Key(Colon(value))) =>
                val decremented = accum.decrement
                decremented.copy(stack = decremented.stack push Colon(value))
              case _ => accum
            }

            buildState(findEndString(buffers, next.bufferIndex, next.offset + 1) map { case (bufferIndex, offset) =>
              // Jump over the string
              BalancedStackState(
                bufferIndex,
                offset + 1,
                next.stack
              )
            } getOrElse {
              // String didn't end
              val lastBuffer = buffers(buffers.length - 1)
              val lastCharacter = lastBuffer.get(lastBuffer.length - 1)
              val quoted = next.stack push Quote
              BalancedStackState(
                buffers.length,
                0,
                if (lastCharacter == '\\') quoted push EscapeChar else quoted
              )
            })

          case _ => buildState(accum.skip)
        }
      }

    buildState(BalancedStackState(0, 0, new Stack())).stack
  }

  // Count braces, quotes and parens in every chunk's CharBuffer.
  // Creates a new buffer which can correctly close the chunk.
  def getJsonCloserBuffer(buffers: Vector[CharBuffer]) = {
    val BlankElement = "null"

    def balancedToString(b: Balanced): String = b match {
      case Brace => "}"
      case Bracket => "]"
      case Colon(v) => ":" + balancedToString(v)
      case Key(v) => "\"null\"" + balancedToString(v)
      case Quote => "\""
      case EscapeChar => "\\"
      case NullValue => "null"
    }

    val stringStack = balancedStack(buffers).map(balancedToString)
    // "}] <- stringStack
    // ,null <- 5
    val lastChar = buffers.last.get(buffers.last.length - 1)
    val needsComma = lastChar != ',' || stringStack.size > 1
    val closerBuffer = CharBuffer.allocate(stringStack.map(_.length).sum + 4 + (if (needsComma) 1 else 0))

    @tailrec def addToCloserBuffer(s: Stack[String]) {
      if (s.length == 1) {
        // Last element should always be a Bracket (']')
        // Put the blank element before end of array
        if (needsComma) {
          closerBuffer.put(',')
        }
        closerBuffer.put(BlankElement)
      }
      if (!s.isEmpty) {
        closerBuffer.put(s.head)
        addToCloserBuffer(s.pop)
      }
    }

    addToCloserBuffer(stringStack)
    closerBuffer.flip()
    closerBuffer
  }
}
