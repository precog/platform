package com.precog.yggdrasil
package iterable

trait OperationsAPI extends StorageEngineQueryComponent with DatasetOpsComponent 

trait StorageEngineQueryComponent {
  type Dataset[E]
  type Grouping[K, A]
  type QueryAPI <: StorageEngineQueryAPI[Dataset[(Identities, Seq[CValue])]]
  val query: QueryAPI
}

trait DatasetOpsComponent {
  type Dataset[E]
  type Memoable[E]
  type Grouping[K, A]
  type Ops <: DatasetOps[Dataset, Memoable, Grouping] with GroupingOps[Dataset, Memoable, Grouping]
  val ops: Ops
}

