package com.reportgrid.storage

import scalaz._
import scalaz.Scalaz._
import scalaz.iteratee._
import scalaz.iteratee.IterateeT._
import scalaz.iteratee.StepT._
import scalaz.iteratee.Input._
import scalaz.iteratee.EnumeratorT._
import scalaz.iteratee.EnumerateeT._
import scalaz.effect._
import scalaz.syntax.order._
import scalaz.syntax.bind._
import java.nio.ByteBuffer

object ProjectionOps {
  def printer[X, E, F[_] : Monad] : IterateeT[X, E, F, Unit] = {
    def loop : Input[E] => IterateeT[X, E, F, Unit] = {
      i => i.fold(el = {e => println(e); cont(loop)},
                  empty = cont(loop),
                  eof = {println("All done"); done((), eofInput)})
    }

    cont(loop)
  }
}
