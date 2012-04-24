package com.precog
package daze

import yggdrasil._

trait TableDatasetOpsComponent extends DatasetOpsComponent {
  type Dataset[α] = Table
  type Memoable[α] = RowView
  type Grouping[α, β] = GroupTable

  trait Ops extends DatasetOps[Dataset, Memoable, Grouping]
}


// vim: set ts=4 sw=4 et:
