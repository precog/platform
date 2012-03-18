
package com.precog.yggdrasil


type TableChunkSchema = Seq[(CPath, CType)]

trait ColumnBuffer {
  def ints: IntBuffer

  def bools: BoolBuffer

  def append(it: RowState, idx: Int): Unit

  def clear: Unit
  // ...
}

class ColumnBufferInt extends ColumnBuffer {
  val ints = new ArrayListInt

  def append(it: RowState, idx: Int): Unit = {
    ints.append(it.intAt(idx))
  }

  def clear = ints.clear
}

case class TableChunkBuffer(schema: TableChunkSchema) extends TableChunk {
  def iterator = ...

  val columns = new Array[ColumnBuffer]
  var length = 0

  private var i = 0

  while (i < schema.length) {
    val ctype = schema(i)._2

    ctype match {
      case CInt => columns(i) = new ColumnBufferInt
      ...
    }

    ++i
  }

  def clear: Unit = {
    length = 0
    var i = 0
    while (i < columns.length) {
      columns(i).clear
      ++i
    }
  }

  def append(rowStates: RowState*): TableChunkBuffer = {
    length += 1
    var i, j = 0
    while (i < rowStates.length) {
      val it = rowStates(i)
      var k = 0
      while (k < it.schema.length) {
        columns(j).append(it, k)
        ++j
        ++k
      }
    }
  }
}

sealed trait CogroupState
case object ReadBothEmpty extends CogroupState
case class ReadLeftSpanLeft(leftSpan: TableChunkBuffer, right: TableChunk) extends CogroupState
case class ReadBothSpanBoth(leftSpan: TableChunkBuffer, rightSpan: TableChunkBuffer) extends CogroupState
case class ReadRightSpanRight(left: TableChunk, rightSpan: TableChunkBuffer) extends CogroupState
case class ReadLeft(left: TableChunk, right: TableChunk) extends CogroupState
case class ReadRight(left: TableChunk, right: TableChunk) extends CogroupState

trait TableChunk { self =>
  def schema: TableChunkSchema

  def iterator: RowIterator

  def isEmpty: Boolean

  // Runtime exception if types are different
  def concat(chunk: TableChunk): TableChunk

  def splitAt(idx: Int): (TableChunk, TableChunk) = (
    new TableChunk {
      def schema = self.schema

      def iterator = self.take(idx)
    },
    new TableChunk {
      def schema = self.schema

      def iterator = self.drop(idx)
    }
  )
}

object TableChunk {
  def empty(schema1: TableChunkSchema): TableChunk = {
    def schema = schema1
    
    def iterator = new RowIteratorEmpty(schema)
    
    def isEmpty = true
  }

  def builder(schema: TableChunkSchema): TableChunkBuilder = new {
  }
}

trait Table {
  def iterator: Iterator[TableChunk]
}

def join(table1: Table, table2: Table)(implicit order: Order[RowState]): Table = {
  def join0(t1: Iterator[TableChunk], t2: Iterator[TableChunk], leftBuffer: TableChunkBuffer, rightBuffer: TableChunkBuffer, state: CogroupState): Iterator[TableChunk] = {


    state match {
      case ReadBothEmpty =>
        leftBuffer.clear
        rightBuffer.clear
        val it1: RowIterator = chunk1.iterator
        val it2: RowIterator = chunk2.iterator

        var more = true

        var leftIdx  = 0
        var rightIdx = 0

        def buildSpans(
        while (more) {
          
            case EQ =>
              leftBuffer.append(it1)
              rightBuffer.append(it2)
              
              it1.advance
              while (order.order(it1, it2) == EQ) {
                leftBuffer.append(it1)
                it1.advance
              }

              it2.advance
              val leftBufIter = leftBuffer.iterator
              while (order.order(leftBufIter, it2) == EQ) {
                rightBuffer.append(it2)
                it2.advance
              }

              if (leftBuffer.length > 1) {
                if (rightBuffer.length > 1) {
                  
                } else {

                }
              } else if (rightBuffer.length > 1) {

              } else {

              }

            case LT => more = it1.next; leftIdx++ 
            case GT => more = it2.next; rightIdx++
          }
        }
    }
  }

  join0(table1.iterator, table2.iterator, ReadBothEmpty)
}


  def cogroup[X, F[_]](identityPrefix: Int)(implicit M: Monad[F]): Enumeratee2T[X, TableChunk, TableChunk, TableChunk, F] =
    new Enumeratee2T[X, TableChunk, TableChunk, TableChunk, F] {
      type Buffer = (TableChunk,TableChunk)
      type ContFunc[A] = Input[TableChunk] => IterateeT[X, TableChunk, F, A]

      /* This method is responsible for grouping two chunks up to the point where either is exhausted.
       * If sides are exhausted but no more input is available it will completely use the chunks. */
      def innerGroup(lBuffer: TableChunk, finalLeft: Boolean, rBuffer: TableChunk, finalRight: Boolean, acc: TableChunk, orderJ: Order[J], orderK: Order[K]): (TableChunk, Option[Buffer]) = {
        //TODO: Use iterators instead of indexed access into the vector
        // Determine where the lead sequence of identical elements ends
        def identityPartitionIndex[E](elements: TableChunk, order: Order[E]): Int =
          if (elements.length == 0) {
            0
          } else {
            var i = 1
            while (i < elements.length && order(elements(0), elements(i)) == EQ) {
              i += 1
            }
            i
          }

        val leftRestIndex = identityPartitionIndex(lBuffer, orderJ)
        val rightRestIndex = identityPartitionIndex(rBuffer, orderK)

        // If we have empty "rest" on either side and this isn't the final group (e.g. input available on that side), we've hit a chunk boundary and need more data
        if ((leftRestIndex == lBuffer.length && ! finalLeft) || (rightRestIndex == rBuffer.length && ! finalRight)) {
          (acc, Some((lBuffer, rBuffer)))
        } else if (lBuffer.length == 0) {
          // We've expired the left side, so we just map the whole right side. This should only be reached when finalLeft or finalRight are true for both sides
          (acc ++ rBuffer.map(Right3(_)), None)
        } else if (rBuffer.length == 0) {
          // We've expired the right side, so we just map the whole left side. This should only be reached when finalLeft or finalRight are true for both sides
          (acc ++ lBuffer.map(Left3(_)), None)
        } else {
          // At this point we compare heads to determine how to group the leading identical sequences from both sides
          order(lBuffer(0), rBuffer(0)) match {
            case EQ => {
              // When equal, we need to generate the cartesian product of both sides
              val buffer = new Array[TableChunk](leftRestIndex * rightRestIndex)
              var i = 0
              while (i < leftRestIndex) {
                var j = 0
                while (j < rightRestIndex) { 
                  buffer(i * rightRestIndex + j) = Middle3(lBuffer(i), rBuffer(j))
                  j += 1
                }
                i += 1
              }
              innerGroup(lBuffer.drop(leftRestIndex), finalLeft, rBuffer.drop(rightRestIndex), finalRight, acc ++ buffer, orderJ, orderK)
            }
            case LT => {
              // Accumulate the left side first and recurse on the remainder of the left side and the full right side
              val buffer = new Array[TableChunk](leftRestIndex)
              var i = 0
              while (i < leftRestIndex) { buffer(i) = Left3(lBuffer(i)); i += 1 }

              innerGroup(lBuffer.drop(leftRestIndex), finalLeft, rBuffer, finalRight, acc ++ buffer, orderJ, orderK)
            }
            case GT => {
              // Accumulate the right side first and recurse on the remainder of the right side and the full left side
              val buffer = new Array[TableChunk](rightRestIndex)
              var i = 0
              while (i < rightRestIndex) { buffer(i) = Right3(rBuffer(i)); i += 1 }

              innerGroup(lBuffer, finalLeft, rBuffer.drop(rightRestIndex), finalRight, acc ++ buffer, orderJ, orderK)
            }
          }
        }
      }

      def outerGroup(left: Option[TableChunk], right: Option[TableChunk], b: Buffer, orderJ: Order[J], orderK: Order[K]): Option[(TableChunk, Option[Buffer])] = {
        val (lBuffer, rBuffer) = b

        (left, right) match {
          case (Some(leftChunk), Some(rightChunk)) => Some(innerGroup(lBuffer ++ leftChunk, false, rBuffer ++ rightChunk, false, TableChunk.empty, orderJ, orderK))
          case (Some(leftChunk), None)             => Some(innerGroup(lBuffer ++ leftChunk, false, rBuffer, true, TableChunk.empty, orderJ, orderK))
          case (None, Some(rightChunk))            => Some(innerGroup(lBuffer, true, rBuffer ++ rightChunk, false, TableChunk.empty, orderJ, orderK))
          // Clean up remaining buffers when we run out of input
          case (None, None) if ! (lBuffer.isEmpty && rBuffer.isEmpty) => Some(innerGroup(lBuffer, true, rBuffer, true, TableChunk.empty, orderJ, orderK))
          case (None, None)                        => None
        }
      }

      def readBoth[A](s: StepM[A], contf: ContFunc[A], buffers: Buffer, orderJ: Order[J], orderK: Order[K]): IterateeT[X, TableChunk, IterateeM, StepM[A]] = 
        for {
          leftOpt  <- head[X, TableChunk, IterateeM]
          rightOpt <- head[X, TableChunk, F].liftI[TableChunk]
          a <- matchOuter[A](s, contf, orderJ, orderK)(outerGroup(leftOpt, rightOpt, buffers, orderJ, orderK))
        } yield a

      def readLeft[A](s: StepM[A], contf: ContFunc[A], buffers: Buffer, orderJ: Order[J], orderK: Order[K]): IterateeT[X, TableChunk, IterateeM, StepM[A]] = 
        for {
          leftOpt  <- head[X, TableChunk, IterateeM]
          a <- matchOuter[A](s, contf, orderJ, orderK)(outerGroup(leftOpt, Some(Vector()), buffers, orderJ, orderK)) // Need to pass an empty buffer on right since this isn't terminal
        } yield a

      def readRight[A](s: StepM[A], contf: ContFunc[A], buffers: Buffer, orderJ: Order[J], orderK: Order[K]): IterateeT[X, TableChunk, IterateeM, StepM[A]] = 
        for {
          rightOpt <- head[X, TableChunk, F].liftI[TableChunk]
          a <- matchOuter[A](s, contf, orderJ, orderK)(outerGroup(Some(Vector()), rightOpt, buffers, orderJ, orderK)) // Need to pass en empty buffer on the left since it isn't terminal
        } yield a

      def matchOuter[A](s: StepM[A], contf: ContFunc[A], orderJ: Order[J], orderK: Order[K]): PartialFunction[Option[(TableChunk, Option[Buffer])], IterateeT[X, TableChunk, IterateeM, StepM[A]]] = {
        case Some((newChunk,newBuffers)) => {
          iterateeT[X, TableChunk, IterateeM, StepM[A]](contf(elInput(Vector(newChunk: _*))) >>== (step(_, newBuffers.getOrElse((ArrayBuffer(), ArrayBuffer())), orderJ, orderK).value))
        }
        case None => done[X, TableChunk, IterateeM, StepM[A]](s, eofInput)
      }

      def step[A](s: StepM[A], buffers: Buffer, orderJ: Order[J], orderK: Order[K]): IterateeT[X, TableChunk, IterateeM, StepM[A]] = {
        s.fold[IterateeT[X, TableChunk, IterateeM, StepM[A]]](
          cont = contf => {
            /* Conditionally read from both sides as needed:
             * 1. No buffer on either side means two reads
             * 2. Buffer on one side means read the other side, and read the existing side if the buffer's first element is the same as its last element
             * 3. Buffer on both sides means read the side that has 1st element == last element */
            (buffers._1.isEmpty, buffers._2.isEmpty) match {
              // No buffer on either side means we need a fresh read on both sides
              case (true, true) => readBoth(s, contf, buffers, orderJ, orderK)
              // Buffer only on one side means a definite read on the other, and conditional on the existing if b.head == b.last
              case (true, right) if orderK.equal(right.head, right.last) => readBoth(s, contf, buffers, orderJ, orderK)
              case (true, right) => readLeft(s, contf, buffers, orderJ, orderK)
              case (left, true)  if orderJ.equal(left.head, left.last)   => readBoth(s, contf, buffers, orderJ, orderK)
              case (left, true) => readRight(s, contf, buffers, orderJ, orderK)
              // Buffer on both sides means conditional on each side having an equivalent series
              case (left, right) if orderJ.equal(left.head, left.last) && orderK.equal(right.head, right.last) => readBoth(s, contf, buffers, orderJ, orderK)
              case (left, _) if orderJ.equal(left.head, left.last) => readLeft(s, contf, buffers, orderJ, orderK)
              case (_, right) if orderK.equal(right.head, right.last) => readRight(s, contf, buffers, orderJ, orderK)
            }
          },
          done = (a, r) => done[X, TableChunk, IterateeM, StepM[A]](sdone(a, if (r.isEof) eofInput else emptyInput), if (r.isEof) eofInput else emptyInput),
          err  = x => err[X, TableChunk, IterateeM, StepM[A]](x)
        )
      }

      def apply[A] = {
        step[A](_, (ArrayBuffer(),ArrayBuffer()), jOrder, kOrder)
      }
    }
}

// vim: set ts=4 sw=4 et:
