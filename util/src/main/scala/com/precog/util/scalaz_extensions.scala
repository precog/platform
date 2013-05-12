package com.precog.util

import scalaz._
import scalaz.StreamT._

package object scalaz_extensions {
  /*
  def mapM[M[_], A, B](stream: StreamT[M, A])(f: A => M[B])(implicit m: Monad[M]): StreamT[M, B] = stepBind(stream) {
    _( yieldd = (a, s) => m.map(f(a)) { Yield(_, mapM(s)(f)) }
     , skip = s => m.point(Skip(mapM(s)(f)))
     , done = m.point(Done)
     )
  }

  private def stepBind[M[_], A, B](stream: StreamT[M, A])(f: Step[A, StreamT[M, A]] => M[Step[B, StreamT[M, B]]])(implicit M: Monad[M]): StreamT[M, B] = 
    StreamT(M.bind(stream.step)(f))
    */
}
