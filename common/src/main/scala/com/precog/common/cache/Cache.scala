package com.precog.common.cache

import com.precog.util.PrecogUnit

import akka.util.Duration

import com.google.common.cache.{Cache => GCache, _}

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
  sealed trait CacheOption[K, V] {
    def apply(builder: CacheBuilder[K, V]): CacheBuilder[K, V]
  }

  case class MaxSize[K, V](size: Long) extends CacheOption[K, V] {
    def apply(builder: CacheBuilder[K, V]) = builder.maximumSize(size)
  }

  case class ExpireAfterAccess[K, V](timeout: Duration) extends CacheOption[K, V] {
    def apply(builder: CacheBuilder[K, V]) = builder.expireAfterAccess(timeout.length, timeout.unit)
  }

  case class ExpireAfterWrite[K, V](timeout: Duration) extends CacheOption[K, V] {
    def apply(builder: CacheBuilder[K, V]) = builder.expireAfterWrite(timeout.length, timeout.unit)
  }

  case class OnRemoval[K, V](onRemove: (K, V, RemovalCause) => PrecogUnit) extends CacheOption[K, V] {
    def apply(builder: CacheBuilder[K, V]) = builder.removalListener(new RemovalListener[K, V] {
      def onRemoval(notification: RemovalNotification[K, V]) = onRemove(notification.getKey, notification.getValue, notification.getCause)
    })
  }

  private def createBuilder[K, V](options: Seq[CacheOption[K, V]]): CacheBuilder[K, V] = 
    options.foldLeft(CacheBuilder.newBuilder.asInstanceOf[CacheBuilder[K, V]]) {
      case (acc, opt) => opt.apply(acc)
    }

  def simple[K, V] (options: CacheOption[K, V]*): SimpleCache[K, V] = {
    new SimpleCache[K, V](createBuilder(options).build())
  }

  def auto[K, V] (options: CacheOption[K, V]*)(loader: K => V): AutoCache[K, V] = {
    val backing = createBuilder(options).build(new CacheLoader[K, V] {
      def load(key : K) = loader(key)
    })
    new AutoCache[K, V](backing)
  }
}
