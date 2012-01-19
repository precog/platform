package com.reportgrid.yggdrasil

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

  def sumSValues[X, F[_]: Monad]: IterateeT[X, SValue, F, Option[SValue]] = {
    foldM[X, SValue, F, Option[SValue]](None) { (acc, sv) => 
      Monad[F].point {
        acc map { asv =>
          sv.mapDoubleOr(asv) {
            d => SDouble(asv.mapDoubleOr(d) { _ + d })
          }
        } orElse {
          sv.mapDoubleOr(Option.empty[SValue]) { _ => Some(sv) }
        }
      }
    }
  }


  // enumE((sumSValues >>== enum).run(x => Monad[F].point(None)))
}
