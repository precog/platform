package com.querio

import scala.collection.immutable.SortedMap
import scalaz.Scalaz._
import scalaz.Semigroup

package object ct {
  implicit def mActions[M[_], A](m: M[A]): Actions[M, A] = new Actions[M, A] {
    override val value = m
  }

  implicit def pf[A](a: A): PF[A] = new PF(a)
  implicit def SortedMapSemigroup[K, V](implicit ss: Semigroup[V]): Semigroup[SortedMap[K, V]] = semigroup {
    (m1, m2) => {
      // semigroups are not commutative, so order may matter. 
      val (from, to, semigroup) = {
        if (m1.size > m2.size) (m2, m1, ss.append(_: V, _: V))
        else (m1, m2, (ss.append(_: V, _: V)).flip)
      }

      from.foldLeft(to) {
        case (to, (k, v)) => to + (k -> to.get(k).map(semigroup(_, v)).getOrElse(v))
      }
    }
  }
}

// vim: set ts=4 sw=4 et:
