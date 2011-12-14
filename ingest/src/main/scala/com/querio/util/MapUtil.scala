package com.querio.util

object MapUtil {
  def merge2[K, V1, V2, Z](m1: Map[K, V1], m2: Map[K, V2])(f: (Option[V1], Option[V2]) => Z): Map[K, Z] = {
    val keys = m1.keys ++ m2.keys
  
    keys.foldLeft(Map[K, Z]()) { (map, key) =>
      val value1 = m1.get(key)
      val value2 = m2.get(key)
    
      map + (key -> f(value1, value2))
    }
  }
  
  def merge2WithDefault[K, V, Z](default: V)(m1: Map[K, V], m2: Map[K, V])(f: (V, V) => Z): Map[K, Z] = {
    merge2WithDefaults(default, default)(m1, m2)(f)
  }

  def merge2WithDefault2[K, V](default: V)(m1: Map[K, V], m2: Map[K, V])(f: (V, V) => V): Map[K, V] = {
    val (mergeFrom, mergeTo, merge) = if (m1.size > m2.size) (m1, m2, f) else (m2, m1, (v2: V, v1: V) => f(v1, v2))

    mergeFrom.foldLeft(mergeTo) {
      case (mergeTo, (k, v)) =>
        mergeTo + (k -> merge(mergeTo.getOrElse(k, default), v))
    }
  }

  
  def merge2WithDefaults[K, V1, V2, Z](default1: V1, default2: V2)(m1: Map[K, V1], m2: Map[K, V2])(f: (V1, V2) => Z): Map[K, Z] = {
    val keys = m1.keys ++ m2.keys
  
    keys.foldLeft(Map[K, Z]()) { (map, key) =>
      val value1 = m1.get(key).getOrElse(default1)
      val value2 = m2.get(key).getOrElse(default2)
    
      map + (key -> f(value1, value2))
    }
  }
  
  def merge3[K, V1, V2, V3, Z](m1: Map[K, V1], m2: Map[K, V2], m3: Map[K, V3])(f: (Option[V1], Option[V2], Option[V3]) => Z): Map[K, Z] = {
    val keys = m1.keys ++ m2.keys
  
    keys.foldLeft(Map[K, Z]()) { (map, key) =>
      val value1 = m1.get(key)
      val value2 = m2.get(key)
      val value3 = m3.get(key)
    
      map + (key -> f(value1, value2, value3))
    }
  }
  
  def merge3WithDefault[K, V, Z](default: V)(m1: Map[K, V], m2: Map[K, V], m3: Map[K, V])(f: (V, V, V) => Z): Map[K, Z] = {
    merge3WithDefaults(default, default, default)(m1, m2, m3)(f)
  }
  
  def merge3WithDefaults[K, V1, V2, V3, Z](default1: V1, default2: V2, default3: V3)(m1: Map[K, V1], m2: Map[K, V2], m3: Map[K, V3])(f: (V1, V2, V3) => Z): Map[K, Z] = {
    val keys = m1.keys ++ m2.keys
  
    keys.foldLeft(Map[K, Z]()) { (map, key) =>
      val value1 = m1.get(key).getOrElse(default1)
      val value2 = m2.get(key).getOrElse(default2)
      val value3 = m3.get(key).getOrElse(default3)
    
      map + (key -> f(value1, value2, value3))
    }
  }
  
  def merge4[K, V1, V2, V3, V4, Z](m1: Map[K, V1], m2: Map[K, V2], m3: Map[K, V3], m4: Map[K, V4])(f: (Option[V1], Option[V2], Option[V3], Option[V4]) => Z): Map[K, Z] = {
    val keys = m1.keys ++ m2.keys
  
    keys.foldLeft(Map[K, Z]()) { (map, key) =>
      val value1 = m1.get(key)
      val value2 = m2.get(key)
      val value3 = m3.get(key)
      val value4 = m4.get(key)
    
      map + (key -> f(value1, value2, value3, value4))
    }
  }
  
  def merge4WithDefault[K, V, Z](default: V)(m1: Map[K, V], m2: Map[K, V], m3: Map[K, V], m4: Map[K, V])(f: (V, V, V, V) => Z): Map[K, Z] = {
    merge4WithDefaults(default, default, default, default)(m1, m2, m3, m4)(f)
  }
  
  def merge4WithDefaults[K, V1, V2, V3, V4, Z](default1: V1, default2: V2, default3: V3, default4: V4)(m1: Map[K, V1], m2: Map[K, V2], m3: Map[K, V3], m4: Map[K, V4])(f: (V1, V2, V3, V4) => Z): Map[K, Z] = {
    val keys = m1.keys ++ m2.keys
  
    keys.foldLeft(Map[K, Z]()) { (map, key) =>
      val value1 = m1.get(key).getOrElse(default1)
      val value2 = m2.get(key).getOrElse(default2)
      val value3 = m3.get(key).getOrElse(default3)
      val value4 = m4.get(key).getOrElse(default4)
    
      map + (key -> f(value1, value2, value3, value4))
    }
  }
  
  def flip[K1, K2, V](m: Map[K1, Map[K2, V]]): Map[K2, Map[K1, V]] = {
    m.foldLeft(Map.empty[K2, Map[K1, V]]) { 
      case (outer, (key1, map)) => 
      
      map.foldLeft(outer) { 
        case (outer, (key2, value)) =>
          val inner: Map[K1, V] = outer.get(key2).getOrElse(Map[K1, V]())
          outer + (key2 -> (inner + (key1 -> value)))
      }
    }
  }
}

