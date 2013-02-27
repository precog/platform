package com.precog.daze

trait SortPushDown extends DAG {
  import dag._

  /**
   * This optimization pushes sorts down in the DAG where possible. The reason
   * for this is to try to encourage the sort to memoizable at run time --
   * smaller inner graphs means there is a higher chance we'll see the node
   * pop-up twice.
   *
   * This particular optimization is incredibly useful when we have some table
   * that is not in sorted identity order (say it is ValueSort'ed) and we then
   * construct a new object that is essentially just derefs of this table,
   * followed by wrapping in objects. This will result in N sorts, where N is
   * the number of keys in the table, which is nuts.
   */
  def pushDownSorts(graph: DepGraph): DepGraph = {
    graph mapDown (rewrite => {
      case s @ Sort(Join(op, CrossLeftSort, left, const @ Const(_)), sortBy) =>
        Join(op, CrossLeftSort, rewrite(Sort(left, sortBy)), const)(s.loc)
      case s @ Sort(Join(op, CrossRightSort, const @ Const(_), right), sortBy) =>
        Join(op, CrossRightSort, const, rewrite(Sort(right, sortBy)))(s.loc)
    })
  }
}
