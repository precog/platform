package com.precog.yggdrasil

abstract class Scanner[@specialized(Long, Double, Boolean) A: Manifest, B: Manifest, @specialized(Long, Double, Boolean) C: Manifest] {
  def zero: B
  
  def apply(a: A, b: B): B
  
  def extract(b: B): C
}
