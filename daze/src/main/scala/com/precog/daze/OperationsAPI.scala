package com.precog
package daze

trait OperationsAPI extends StorageEngineQueryComponent with DatasetOpsComponent 

trait StorageEngineQueryComponent {
  type Dataset[E]
  type Grouping[K, A]
  type QueryAPI <: StorageEngineQueryAPI[Dataset]
  val query: QueryAPI
}

trait DatasetOpsComponent {
  type Dataset[E]
  type Valueset[E]
  type Grouping[K, A]
  type Ops <: DatasetOps[Dataset, Valueset, Grouping] with GroupingOps[Dataset, Valueset, Grouping]
  val ops: Ops
}

