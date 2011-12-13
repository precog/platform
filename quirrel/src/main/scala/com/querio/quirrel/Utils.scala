package com.querio.quirrel

object Utils {
  def merge[A, B](first: Map[A, Set[B]], second: Map[A, Set[B]]): Map[A, Set[B]] = {
    second.foldLeft(first) {
      case (acc, (key, set)) if acc contains key => acc.updated(key, acc(key) ++ set)
      case (acc, pair) => acc + pair
    }
  }
}
