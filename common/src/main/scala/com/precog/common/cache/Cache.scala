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
package com.precog.common.cache

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache, Cache => GCache}
import java.util.concurrent.ExecutionException

class Cache[K,V] (private val backing: GCache[K,V]) {
  def getIfPresent(key: K): Option[V] = Option(backing.getIfPresent(key))
  def put(key: K, value: V): Unit = backing.put(key, value)
}

object Cache {
 def apply[K, V] (size: Int): Cache[K, V] = {
   val builder: CacheBuilder[K, V] = CacheBuilder.newBuilder.asInstanceOf[CacheBuilder[K, V]]
   new Cache[K, V](builder.maximumSize(size).build())
  }
}
