package com.precog
package yggdrasil

trait OperationsAPI extends StorageEngineQueryComponent with DatasetOpsComponent 

trait StorageEngineQueryComponent {
  type Dataset[E]
  type Grouping[K, A]
  type QueryAPI <: StorageEngineQueryAPI[Dataset]
  val query: QueryAPI
}

trait DatasetOpsComponent {
  type Dataset[E]
  type Memoable[E]
  type Grouping[K, A]
  type Ops <: DatasetOps[Dataset, Memoable, Grouping] with GroupingOps[Dataset, Memoable, Grouping]
  val ops: Ops
}

