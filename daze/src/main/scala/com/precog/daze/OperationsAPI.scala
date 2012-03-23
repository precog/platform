package com.precog
package daze

trait OperationsAPI extends StorageEngineQueryComponent with DatasetOpsComponent 

trait StorageEngineQueryComponent {
  type Dataset[E]
  type Grouping[K, A]
  type QueryAPI <: StorageEngineQueryAPI[Dataset]
  def query: QueryAPI
}

trait DatasetOpsComponent {
  type Dataset[E]
  type Grouping[K, A]
  type Ops <: DatasetOps[Dataset, Grouping]
  def ops: Ops
}

