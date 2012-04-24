package com.precog.yggdrasil

trait CogroupMerge {
  def apply[@specialized(Boolean, Long, Double) A](ref: VColumnRef[A]): Option[F2P[A, A, A]]
}

object CogroupMerge {
  object second extends CogroupMerge {
    def apply[@specialized(Boolean, Long, Double) A](ref: VColumnRef[A]): Option[F2P[A, A, A]] = {
      Some(
        new F2P[A, A, A] { 
          val returns = ref.ctype
          val accepts = (ref.ctype, ref.ctype)
          def isDefinedAt(a1: A, a2: A) = true
          def apply(a1: A, a2: A) = a2 
        }
      )
    }
  }
}

// vim: set ts=4 sw=4 et:
