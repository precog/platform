package com.precog.yggdrasil

trait FNModule {
  type F1 
  type F2

  implicit def liftF1(f1: F1): F1Like 
  trait F1Like {
    def compose(f1: F1): F1
    def andThen(f1: F1): F1
  }

  implicit def liftF2(f2: F2): F2Like 
  trait F2Like {
    def applyl(cv: CValue): F1 
    def applyr(cv: CValue): F1

    def andThen(f1: F1): F2
  }
}


// vim: set ts=4 sw=4 et:
