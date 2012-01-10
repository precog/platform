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
  /*
  def matcher[X, E: Ordering, F[_]: Monad, A](left: EnumeratorT[X, E, F, Option[E]], right: EnumeratorT[X, E, F, Option[E]]): EnumeratorT[X, (E, E), F, A] = {
    val FMonad = implicitly[Monad[F]]
    val ENone = FMonad.point(Option.empty[E])

    def loop : EnumeratorT[X, (E,E), F, A] = {
      s => {
        s.mapContOr(
          k => {
            FMonad.map((head[X,E,F] >>== left).apply(_ => ENone)) {
              case Some(e1) => {
                (dropWhile[X,E,F](e2 => e2.compare(e1) < 0).map(_ => Option.empty[E]) >>== right).apply(_ => ENone)
                FMonad.map((peek[X,E,F] >>== right).apply( _ => ENone)) {
                  case Some(e2) if e1 == e2 => {
                    // If we matched we need to consume the element from right
                    (drop[X,E,F](1).map{ _ => Option.empty[E] }  >>== right).apply(_ => ENone)
                    k(elInput((e1,e2))) >>== loop
                  }
                  case _ => k(emptyInput) >>== loop
                }
              }
              case None => {
                // eof, so we need to close the enumerators and indicate overall eof
                k(eofInput)
              }
            }
          }
        ,s.pointI)
      }
    }

    loop
  }
  */

  def arrayEnum[X, E, F[_] : Monad, A](a : Array[E]) : EnumeratorT[X, E, F, A] = {
    def loop(pos : Int) : EnumeratorT[X, E, F, A] = {
      s => s.mapContOr(
        k => {
          if (pos == a.length) {
            k(eofInput)
          } else {
            k(elInput(a(pos))) >>== loop(pos + 1)
          }
        },
        s.pointI
      )
    }

    loop(0)
  }

  def printer[X, E, F[_] : Monad] : IterateeT[X, E, F, Unit] = {
    def loop : Input[E] => IterateeT[X, E, F, Unit] = {
      i => i.fold(el = {e => println(e); cont(loop)},
                  empty = cont(loop),
                  eof = {println("All done"); done((), eofInput)})
    }

    cont(loop)
  }

  def sum[X, E : Numeric, F[_] : Monad] : IterateeT[X, E, F, E] = {
    val num = implicitly[Numeric[E]]
    def loop(a : E) : Input[E] => IterateeT[X, E, F, E] = {
      i => i.fold(el = e => cont(loop(num.plus(a, e))),
                  empty = cont(loop(a)),
                  eof = done(a, eofInput))
    }

    cont(loop(num.zero))
  }

  def loop[X, E: Order, F[_]: Monad, A](step: StepT[X, (E, E), F, A]): IterateeT[X, E, ({type L[A] = IterateeT[X, E, F, A]})#L, StepT[X, (E, E), F, A]] = {
    step.fold(
      cont = (contf: Input[(E, E)] => IterateeT[X, (E, E), F, A]) => {
        for {
          leftHeadOpt  <- head[X, E, ({type L[A] = IterateeT[X, E, F, A]})#L]
          rightStep    <- leftHeadOpt.map { leftHead => println("l:" + leftHead)
                            IterateeT.IterateeTMonadTrans[X, E].liftM[({type L[A] = IterateeT[X, E, F, A]})#L, StepT[X, (E, E), F, A]](
                              for {
                                _       <- dropWhile[X, E, F](_ < leftHead)
                                peekOpt <- peek[X, E, F]
                                step2   <- (peekOpt match {
                                  case Some(rightPeek) if leftHead < rightPeek => done(step, emptyInput[E])
                                  case Some(rightPeek) => IterateeT(contf(elInput((leftHead, rightPeek))).value.map(sdone(_, emptyInput[E])))
                                  case None => IterateeT(contf(eofInput[(E, E)]).value.map(sdone(_, eofInput[E])))
                                }): IterateeT[X, E, F, StepT[X, (E, E), F, A]]
                              } yield step2
                            )
                          } getOrElse {
                            IterateeT.IterateeTMonadTrans[X, E].liftM[({type L[A] = IterateeT[X, E, F, A]})#L, StepT[X, (E, E), F, A]](
                              IterateeT(contf(eofInput[(E, E)]).value.map(sdone(_, eofInput[E])))
                            )
                          }
        } yield rightStep
      },
      done = (a, i) => done[X, E, ({type L[A] = IterateeT[X, E, F, A]})#L, StepT[X, (E, E), F, A]](sdone(a, i), emptyInput[E]),
      err = x => err[X, E, ({type L[A] = IterateeT[X, E, F, A]})#L, StepT[X, (E, E), F, A]](x)
    )
  }

  def testStep[X, E, F[_]: Monad]: StepT[X, (E, E), F, List[(E, E)]] = {
    def testLoop: Input[(E, E)] => IterateeT[X, (E, E), F, List[(E, E)]] = (_: Input[(E, E)]).fold[IterateeT[X, (E, E), F, List[(E, E)]]](
      el = e => cont(testLoop).map(e :: _),
      empty = cont(testLoop),
      eof = done(Nil, emptyInput[(E, E)])
    )

    scont(testLoop)
  }

  implicit val v = IterateeTMonad[Unit, Int, Id]
  val enum  = enumStream[Unit, Int, ({type L[A] = IterateeT[Unit, Int, Id, A]})#L, StepT[Unit, (Int, Int), Id, List[(Int, Int)]]](Stream(1, 2, 3))
  val enum2 = enumStream[Unit, Int, ({type L[A] = IterateeT[Unit, Int, Id, A]})#L, StepT[Unit, (Int, Int), Id, List[(Int, Int)]]](Stream(1, 3, 5))
  //val enum2 = enumStream[Unit, Int, Id, StepT[Unit, (Int, Int), Id, List[(Int, Int)]]](Stream(1, 2, 3))

  def runIter(tstep : StepT[Unit, (Int,Int), Id, List[(Int,Int)]]) = {
    //val tstep = testStep[Unit, Int, Id]
    //val firstRun: String = loop(tstep).bindThrough(enum)(v).value//.flatMap(loop).bindThrough(enum2)
    /*val secondRun = */
    //secondRun
    "Foo!"
  }
      /*  
  def matchOrdered[X, E: Order, F[_]: Pointed, A](left: EnumeratorT[X, E, F, A], right: EnumeratorT[X, E, F, A]): EnumeratorT[X, (E, E), F, A] = { 
    import scalaz.syntax.order._
    (s: StepT[X, (E, E), F, A]) => s.fold(
      cont = (tf: Input[(E, E)] => IterateeT[X, (E, E), F, A]) => {
        tf(elInput(ell, elr))    
      },  
        lin.fold[IterateeT[X, E, F, A]](
          el = ell => right {
            cont[X, E, F, Iteratee[X, (E, E), F, A]] { (rin: Input[E]) => 
              rin.fold[IterateeT[X, E, F, A]](
                el = elr => {
                  if (ell < elr) {
                    sys.error("todo"): IterateeT[X, (E, E), F, A]
                  } else if (ell > elr) {
                    sys.error("todo"): IterateeT[X, (E, E), F, A]
                  } else {
                    tf(elInput((ell, elr)))
                  }
                },
                empty = s.pointI,
                eof   = tf(eofInput[(E, E)])
              )
            }
          },
          empty = s.pointI,
          eof   = tf(eofInput[(E, E)]) 
        )
      done = sys.error("todo"),
      err  = sys.error("todo") 
    )   
  }
        */


  /*
  def matcher[X, E: Ordering, F[_]: Monad, A]: EnumerateeT[X, E, (E, E), ({type L[A] = IterateeT[X, E, F, A]})#L, A] = (s: StepT[X, (E, E), ({type L[A] = IterateeT[X, E, F, A]})#L, A]) => {
    val FMonad = implicitly[Monad[F]]

    s.fold[IterateeT[X, E, ({type L[A] = IterateeT[X, E, F, A]})#L, StepT[X, E, ({type L[A] = IterateeT[X, E, F, A]})#L, A]]](
      cont = (f: Input[E] => IterateeT[X, E, ({type L[A] = IterateeT[X, E, F, A]})#L, A]) => IterateeT {
        FMonad.point {
          StepT.scont((in: Input[E]) => in.fold(
            empty = sys.error("todo"),
            el = sys.error("todo"),
            eof = sys.error("todo")
          )) : IterateeT[X, E, ({type L[A] = IterateeT[X, E, F, A]})#L, StepT[X, E, ({type L[A] = IterateeT[X, E, F, A]})#L, A]]
        }
      },
      done = sys.error("todo") : (=> A, => Input[E]) => IterateeT[X, E, ({type L[A] = IterateeT[X, E, F, A]})#L, StepT[X, E, ({type L[A] = IterateeT[X, E, F, A]})#L, A]],
      err  = sys.error("todo") : (=> X) => IterateeT[X, E, ({type L[A] = IterateeT[X, E, F, A]})#L, StepT[X, E, ({type L[A] = IterateeT[X, E, F, A]})#L, A]]
    )
  }

  def matchIterT[X, E: Ordering, F[_] : Monad, A, B](left : IterateeT[X, E, F, A], right: IterateeT[X, E, F, A]) : IterateeT[X, (E, E), F, A] = {
    val FMonad = implicitly[Monad[F]]

    def loop(x: IterateeT[X, E, F, A], y: IterateeT[X, E, F, A])(in: Input[(E, E)]): IterateeT[X, (E, E), F, A] = in.fold(
      el = {
        //case (ell, elr) if ell > elr => cont(loop(x, dropUntil(ell <= (_: E)) >> y))
        //case (ell, elr) if ell < elr => cont(loop(dropUntil((_: E) >= elr) >> x, y))
        case (ell, elr) => iterateeT {
          FMonad.point(
            (s: StepT[X, (E, E), F, A]) => {
              s.fold(
                cont = k => k(Input((ell, elr))),
                done = (a, i) => FMonad.point(done(a, i)),
                err  = e => err(e)
              )
            }
          )
        }
      },
      empty = cont(loop(x, y)),
      eof = cont(loop(x, y))
    )

    cont(loop(left, right))
  }
  */
}
