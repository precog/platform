package com.reportgrid.storage

trait StorageEngine {
  /** Streams values, by order of identity.
   */
  def stream[I: Reified, A: Reified](dataset: Expr[Dataset[I, A]], cb: Set[(I, A)] => Boolean): Unit

  def reduce[I: Reified, A: Reified, Z](dataset: Expr[Dataset[I, A]], state: Z)(f: (Z, A) => Z): Z

  def save[A: Reified, IA, B: Reified, IB: Reified]
    (dataset: Expr[Dataset[IA, A]], f: (IA, A) => (IB, B), target: DatasetId): Unit

  def insert[A: Reified](dataset: DatasetId, values: Set[A]): Unit

  def insert[A: Reified, I: Reified](target: DatasetId, values: Set[(I, A)]): Unit

  def move[A: Reified, I: Reified](source: DatasetId, target: DatasetId): Unit

  def delete[A: Reified, I: Reified](source: DatasetId): Unit

  def get[A: Reified, I: Reified](source: DatasetId, ids: Set[I]): Set[(I, A)]
}