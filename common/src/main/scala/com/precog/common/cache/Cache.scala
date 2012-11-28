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
