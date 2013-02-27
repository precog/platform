/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
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
