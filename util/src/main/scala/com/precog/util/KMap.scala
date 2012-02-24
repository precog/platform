package com.precog.util

trait KMap[K[_], V[_]] {
  def +[A](kv: (K[A], V[A])): KMap[K, V]
  def -[A](k: K[A]): KMap[K, V]
  def get[A](k: K[A]): Option[V[A]]
  def isDefinedAt[A](k: K[A]): Boolean
}

object KMap {
  private case class CastMap[K[_], V[_]](delegate: Map[Any, Any]) extends KMap[K, V] {
    def +[A](kv: (K[A], V[A])): KMap[K, V] = CastMap(delegate + kv)
    def -[A](k: K[A]): KMap[K, V] = CastMap(delegate - k)
    def get[A](k: K[A]): Option[V[A]] = delegate.get(k).map(_.asInstanceOf[V[A]])
    def isDefinedAt[A](k: K[A]): Boolean = delegate.isDefinedAt(k)
  }

  def empty[K[_], V[_]]: KMap[K, V] = CastMap(Map())
}

// vim: set ts=4 sw=4 et:
