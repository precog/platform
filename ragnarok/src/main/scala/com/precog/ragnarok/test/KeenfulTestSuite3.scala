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
package com.precog
package ragnarok
package test

object KeenfulTestSuite3 extends PerfTestSuite {
  query(
    """
import std::stats::rank
data := //keenful

data' := data where data.action.verb = "view"

byItem := solve 'item_id, 'item_url 
  data'' := data' where data'.item.id = 'item_id & data'.item.url = 'item_url
  {
    item_id: 'item_id, 
    count: count(data''), 
    item_url : 'item_url
  }  

byItem' := byItem with {rank : rank(byItem.count)}

byItem' where byItem'.rank > max(byItem'.rank - 5)
    """)
}
