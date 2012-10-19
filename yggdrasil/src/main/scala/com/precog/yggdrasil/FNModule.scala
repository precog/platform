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

trait FNDummyModule extends FNModule {
  type F1 = table.CF1
  type F2 = table.CF2

  implicit def liftF1(f: F1) = new F1Like {
    def compose(f1: F1) = f compose f1
    def andThen(f1: F1) = f andThen f1
  }

  implicit def liftF2(f: F2) = new F2Like {
    def applyl(cv: CValue) = new table.CF1(f(table.Column.const(cv), _))
    def applyr(cv: CValue) = new table.CF1(f(_, table.Column.const(cv)))

    def andThen(f1: F1) = new table.CF2((c1, c2) => f(c1, c2) flatMap f1.apply)
  }
}

// vim: set ts=4 sw=4 et:
