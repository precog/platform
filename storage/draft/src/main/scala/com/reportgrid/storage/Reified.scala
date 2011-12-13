package com.reportgrid.storage

trait Dataset[I, A]

sealed trait Reified[A]

object Reified {
  class OptionT[A: Reified] extends Reified[Option[A]] {
    def reified = implicitly[Reified[A]]
  }
  object OptionT {
    def apply[A: Reified] = new OptionT[A]
  }
  implicit def ToReifiedOption[A: Reified] = OptionT[A]
  implicit object BooleanT extends Reified[Boolean]
  implicit object LongT extends Reified[Long]
  class Tuple2T[A: Reified, B: Reified] extends Reified[(A, B)] {
    def reified1 = implicitly[Reified[A]]

    def reified2 = implicitly[Reified[B]]
  }
  object Tuple2T {
    def apply[A: Reified, B: Reified] = new Tuple2T[A, B]
  }
  implicit def ToReifiedTuple2[A: Reified, B: Reified] = Tuple2T[A, B]
  class DatasetT[I: Reified, A: Reified] extends Reified[Dataset[I, A]] {
    def reified1 = implicitly[Reified[I]]

    def reified2 = implicitly[Reified[A]]
  }
  object DatasetT {
    def apply[I: Reified, A: Reified] = new DatasetT[I, A]
  }
  implicit def ToReifiedDataset[I: Reified, A: Reified] = DatasetT[I, A]
}