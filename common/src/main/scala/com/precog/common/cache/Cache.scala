package com.precog.common.cache

import akka.util.Duration

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache, Cache => GCache}

import java.util.concurrent.ExecutionException

import scalaz.{Failure, Success, Validation}

import scala.collection.JavaConverters._
import scala.collection.mutable.Map

class SimpleCache[K, V] (private val backing: GCache[K, V]) extends Map[K, V] {
  def += (kv: (K, V)) = { backing.put(kv._1, kv._2); this }
  def -= (key: K) = { backing.invalidate(key); this }
  def get(key: K): Option[V] = Option(backing.getIfPresent(key))
  def iterator: Iterator[(K, V)] = backing.asMap.entrySet.iterator.asScala.map { kv => (kv.getKey, kv.getValue) }
}

class AutoCache[K, V] (private val backing: LoadingCache[K, V]) extends Map[K, V] {
  def += (kv: (K, V)) = { backing.put(kv._1, kv._2); this }
  def -= (key: K) = { backing.invalidate(key); this }
  def get (key: K): Option[V] = getFull(key).toOption
  def iterator: Iterator[(K, V)] = backing.asMap.entrySet.iterator.asScala.map { kv => (kv.getKey, kv.getValue) }

  def getFull(key: K): Validation[Throwable, V] = Validation.fromTryCatch {
    backing.get(key)
  }
}


object Cache {
  sealed trait CacheOption {
    def apply[K,V](builder: CacheBuilder[K, V]): CacheBuilder[K, V]
  }

  case class MaxSize(size: Long) extends CacheOption {
    def apply[K,V](builder: CacheBuilder[K, V]) = builder.maximumSize(size)
  }

  case class ExpireAfterAccess(timeout: Duration) extends CacheOption {
    def apply[K,V](builder: CacheBuilder[K, V]) = builder.expireAfterAccess(timeout.length, timeout.unit)
  }

  case class ExpireAfterWrite(timeout: Duration) extends CacheOption {
    def apply[K,V](builder: CacheBuilder[K, V]) = builder.expireAfterWrite(timeout.length, timeout.unit)
  }

  private def createBuilder[K, V](options: Seq[CacheOption]): CacheBuilder[K, V] = 
    options.foldLeft(CacheBuilder.newBuilder.asInstanceOf[CacheBuilder[K, V]]) {
      case (acc, opt) => opt.apply(acc)
    }

  def simple[K, V] (options: CacheOption*): SimpleCache[K, V] = {
    new SimpleCache[K, V](createBuilder(options).build())
  }

  def auto[K, V] (options: CacheOption*)(loader: K => V): AutoCache[K, V] = {
    val backing = createBuilder(options).build(new CacheLoader[K, V] {
      def load(key : K) = loader(key)
    })
    new AutoCache[K, V](backing)
  }
}
