package com.precog
package daze

trait OperationsAPI extends StorageEngineQueryComponent with DatasetEnumOpsComponent

trait StorageEngineQueryComponent {
  type QueryAPI <: StorageEngineQueryAPI
  def query: QueryAPI
}

trait DatasetEnumOpsComponent {
  type Ops <: DatasetEnumOps
  def ops: Ops
}

