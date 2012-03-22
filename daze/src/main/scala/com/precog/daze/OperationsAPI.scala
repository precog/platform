package com.precog
package daze

trait OperationsAPI extends StorageEngineQueryComponent with DatasetOpsComponent 

trait StorageEngineQueryComponent {
  type Dataset[E]
  type QueryAPI <: StorageEngineQueryAPI[Dataset]
  def query: QueryAPI
}

trait DatasetOpsComponent {
  type Dataset[E]
  type Ops <: DatasetOps[Dataset]
  def ops: Ops
}

