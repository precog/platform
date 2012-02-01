package com.precog.ct

class PF[A](a: A) {
  def option[B](pf: PartialFunction[A, B]): Option[B] = pf.lift.apply(a)
}


// vim: set ts=4 sw=4 et:
