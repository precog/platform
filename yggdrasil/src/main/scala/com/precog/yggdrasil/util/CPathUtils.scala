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
package com.precog.yggdrasil
package util

import com.precog.common.json._
import blueeyes.json._

import scala.collection.mutable
import scala.annotation.tailrec

import scalaz._
import scalaz.Ordering.{ LT, EQ, GT }


sealed trait CPathPosition
case class CPathPoint(node: CPathNode) extends CPathPosition
case class CPathRange(nodes: Set[CPathNode], start: Int, end: Option[Int]) extends CPathPosition

object CPathPosition {
  def contains(ps: List[CPathPosition], path: CPath): Boolean = {

    @tailrec
    def loop(ps: List[CPathPosition], ns: List[CPathNode]): Boolean = (ps, ns) match {
      case (CPathPoint(p) :: ps, n :: ns) if p == n => loop(ps, ns)
      case (CPathRange(ms, _, _) :: ps, n :: ns) if ms contains n => loop(ps, ns)
      case (Nil, Nil) => true
      case _ => false
    }

    loop(ps, path.nodes)
  }

  implicit object CPathPositionOrder extends Order[CPathPosition] {
    import scalaz.std.int._

    private val nodeOrder = implicitly[Order[CPathNode]]

    def order(p1: CPathPosition, p2: CPathPosition): Ordering = (p1, p2) match {
      case (CPathPoint(CPathIndex(i)), CPathRange(_, l, r)) =>
        if (i < l) LT else if (i > l) GT else EQ //if (r map (_ == i) getOrElse false) EQ else GT
      case (CPathRange(_, l, r), CPathPoint(CPathIndex(i))) =>
        if (i < l) GT else if (i > l) LT else EQ //if (r map (_ == i) getOrElse false) EQ else LT
      case (CPathRange(_, l1, r1), CPathRange(_, l2, r2)) =>
        if (l1 < l2) LT else if (l2 < l1) GT else {
          (r1, r2) match {
            case (Some(r1), Some(r2)) => implicitly[Order[Int]].order(r1, r2)
            case (None, None) => EQ
            case (_, None) => LT
            case (None, _) => GT
          }
        }
      case (CPathPoint(a), CPathPoint(b)) => nodeOrder.order(a, b)
      case (CPathPoint(a), CPathRange(_, i, _)) => nodeOrder.order(a, CPathIndex(i))
      case (CPathRange(_, i, _), CPathPoint(a)) => nodeOrder.order(CPathIndex(i), a)
    }
  }

  def pretty(ps: List[CPathPosition]): String = {
    ps map {
      case CPathRange(_, 0, None) => "[*]"
      case CPathRange(_, i, None) => "[%d+]" format i
      case CPathRange(_, i, Some(j)) if i == j  => "[%d]" format i
      case CPathRange(_, i, Some(j)) => "[%d..%d]" format (i, j)
      case CPathPoint(a) => a.toString
    } mkString ""
  }

  /**
   * Returns a `p` as a positioned CPath.
   */
  def position(p: CPath): List[CPathPosition] = p.nodes map {
    case CPathArray => CPathRange(Set(CPathArray), 0, None)
    case node => CPathPoint(node)
  }

  private def overlaps(l1: Int, r1: Option[Int], l2: Int, r2: Option[Int]): Boolean = {
    (l2 >= l1 && l2 <= r1.getOrElse(l2)) || (l1 >= l2 && l1 <= r2.getOrElse(l1))
  }

  /**
   * This splits 2 positioned CPaths into a list of completely disjoint positioned CPaths.
   */
  def disjoint(c1: List[CPathPosition], c2: List[CPathPosition]): List[List[CPathPosition]] = {
    import scalaz.syntax.apply._
    import scalaz.std.option._

    // case 1: c1 and c2 are disjoint, just return List(c1, c2)
    // case 2: c1 and c2 overlap, return non-overlapping regions of c1 & c2 and the intersection of c1 & c2.
    @tailrec
    def loop(ps: List[CPathPosition], qs: List[CPathPosition], is: List[CPathPosition], rss: List[List[CPathPosition]]): List[List[CPathPosition]] = (ps, qs) match {
      case (Nil, Nil) =>
        is.reverse :: rss

      case (p :: ps, q :: qs) if p == q =>
        loop(ps, qs, p :: is, rss)

      case (CPathPoint(n @ CPathIndex(i)) :: ps, CPathRange(ns, j, k) :: qs) if i >= j && i <= k.getOrElse(i) =>
        val rss0 = if (j < i) {
          ((CPathRange(ns, j, Some(i - 1)) :: is) reverse_::: qs) :: rss
        } else rss

        val rss1 = if (k map (_ > i) getOrElse true) {
          ((CPathRange(ns, i + 1, k) :: is) reverse_::: qs) :: rss0
        } else rss0

        loop(ps, qs, CPathRange(ns + n, i, Some(i)) :: is, rss1)

      case (CPathRange(ns, j, k) :: ps, CPathPoint(n @ CPathIndex(i)) :: qs) if i >= j && i <= k.getOrElse(i) =>
        val rss0 = if (j < i) {
          ((CPathRange(ns, j, Some(i - 1)) :: is) reverse_::: ps) :: rss
        } else rss

        val rss1 = if (k map (_ > i) getOrElse true) {
          ((CPathRange(ns, i + 1, k) :: is) reverse_::: ps) :: rss0
        } else rss0

        loop(ps, qs, CPathRange(ns + n, i, Some(i)) :: is, rss1)

      case (CPathRange(ns1, l1, r1) :: ps, CPathRange(ns2, l2, r2) :: qs) if overlaps(l1, r1, l2, r2) =>
        val rss0 = if (l1 < l2) {
          ((CPathRange(ns1, l1, Some(l2 - 1)) :: is) reverse_::: ps) :: rss
        } else if (l2 < l1) {
          ((CPathRange(ns2, l2, Some(l1 - 1)) :: is) reverse_::: qs) :: rss
        } else {
          rss
        }

        val rss1 = (r1, r2) match {
          case (r1, Some(r2)) if r1 map (_ > r2) getOrElse true =>
            ((CPathRange(ns1, r2 + 1, r1) :: is) reverse_::: ps) :: rss0
          case (Some(r1), r2) if r2 map (_ > r1) getOrElse true =>
            ((CPathRange(ns2, r1 + 1, r2) :: is) reverse_::: qs) :: rss0
          case _ =>
            rss0
        }

        val l = math.max(l1, l2)
        val r = ^(r1, r2)(math.min(_, _))
        loop(ps, qs, CPathRange(ns1 ++ ns2, math.max(l1, l2), ^(r1, r2)(math.min(_, _)) orElse r1 orElse r2) :: is, rss1)

      case (ps, qs) =>
        List(is reverse_::: ps, is reverse_::: qs)
    }

    loop(c1, c2, Nil, Nil)
  }

  def intersect(as: List[CPathPosition], bs: List[CPathPosition]): Boolean = {

    @tailrec
    def loop(as: List[CPathPosition], bs: List[CPathPosition]): Boolean = (as, bs) match {
      case (a :: as, b :: bs) if a == b => loop(as, bs)
      case (CPathPoint(CPathIndex(i)) :: as, CPathRange(_, j, k) :: bs) if i >= j && i <= k.getOrElse(i) => loop(as, bs)
      case (CPathRange(_, j, k) :: as, CPathPoint(CPathIndex(i)) :: bs) if i >= j && i <= k.getOrElse(i) => loop(as, bs)
      case (CPathRange(_, l1, r1) :: as, CPathRange(_, l2, r2) :: bs) if overlaps(l1, r1, l2, r2) => loop(as, bs)
      case (Nil, Nil) => true
      case _ => false
    }

    loop(as, bs)
  }

  /**
   * Returns a way to traverse `paths` safely, in sorted order.
   */
  def traversal(paths: List[CPath]): List[List[CPathPosition]] = {
    import scalaz.std.list._

    val pq = mutable.PriorityQueue[List[CPathPosition]](paths map (position(_)): _*)(implicitly[Order[List[CPathPosition]]].reverseOrder.toScalaOrdering)

    def rec(a: List[CPathPosition], ts: List[List[CPathPosition]]): List[List[CPathPosition]] = {
      if (!pq.isEmpty) {
        val b = pq.dequeue()
        println("Comparing:\n%s\n%s\n\n" format (a, b))
        if (intersect(a, b)) {
          pq.enqueue(disjoint(a, b): _*)
          rec(pq.dequeue(), ts)
        } else {
          rec(b, a :: ts)
        }
      } else {
        (a :: ts).reverse
      }
    }

    if (pq.isEmpty) Nil else rec(pq.dequeue(), Nil)
  }
}


object CPathUtils {
  def cPathToJPaths(cpath: CPath, value: CValue): List[(JPath, CValue)] = (cpath.nodes, value) match {
    case (CPathField(name) :: tail, _) => addComponent(JPathField(name), cPathToJPaths(CPath(tail), value))
    case (CPathIndex(i) :: tail, _) => addComponent(JPathIndex(i), cPathToJPaths(CPath(tail), value))
    case (CPathArray :: tail, CArray(elems, CArrayType(elemType))) =>
      elems.toList.zipWithIndex flatMap { case (e, i) => addComponent(JPathIndex(i), cPathToJPaths(CPath(tail), elemType(e))) }
    // case (CPathMeta(_) :: _, _) => Nil
    case (Nil, _) => List((JPath.Identity, value))
    case (path, _) => sys.error("Bad news, bob! " + path)
  }

  private def addComponent(c: JPathNode, xs: List[(JPath, CValue)]): List[(JPath, CValue)] = xs map {
    case (path, value) => (JPath(c :: path.nodes), value)
  }

  /**
   * Returns the intersection of `cPath1` and `cPath2`. If there are no
   * `CPathArray` components in the 2 paths, then the intersection is non-empty
   * iff `cPath1 === cPath2`. However, if `cPath1` and/or `cPath2` contain some
   * `CPathArray` components, then they intersect if we can replace some of the
   * `CPathArray`s with `CPathIndex(i)` and have them be equal. This is `CPath`
   * is their intersection.
   *
   * For instance, `intersect(CPath("a.b[*].c[0]"), CPath(CPath("a.b[3].c[*]")) === CPath("a.b[3].c[0]")`.
   */
  def intersect(cPath1: CPath, cPath2: CPath): Option[CPath] = {

    @scala.annotation.tailrec
    def loop(ps1: List[CPathNode], ps2: List[CPathNode], matches: List[CPathNode]): Option[CPath] = (ps1, ps2) match {
      case (Nil, Nil) =>
        Some(CPath(matches.reverse))
      case (p1 :: ps1, p2 :: ps2) if p1 == p2 =>
        loop(ps1, ps2, p1 :: matches)
      case (CPathArray :: ps1, (p2: CPathIndex) :: ps2) =>
        loop(ps1, ps2, p2 :: matches)
      case ((p1: CPathIndex) :: ps1, CPathArray :: ps2) =>
        loop(ps1, ps2, p1 :: matches)
      case _ =>
        None
    }

    loop(cPath1.nodes, cPath2.nodes, Nil)
  }


  // TODO Not really a union.
  def union(cPath1: CPath, cPath2: CPath): Option[CPath] = {
    def loop(ps1: List[CPathNode], ps2: List[CPathNode], acc: List[CPathNode]): Option[CPath] = (ps1, ps2) match {
      case (Nil, Nil) =>
        Some(CPath(acc.reverse))
      case (p1 :: ps1, p2 :: ps2) if p1 == p2 =>
        loop(ps1, ps2, p1 :: acc)
      case (CPathArray :: ps1, (_: CPathIndex) :: ps2) =>
        loop(ps1, ps2, CPathArray :: acc)
      case ((_: CPathIndex) :: ps1, CPathArray :: ps2) =>
        loop(ps1, ps2, CPathArray :: acc)
      case _ =>
        None
    }

    loop(cPath1.nodes, cPath2.nodes, Nil)
  }


  /**
   * Returns a list of possible "next" `CPath`s from `cPath`, by incrementing
   * `CPathIndex`es. If `cpath == CPath("a[1].b[3].c[0]")`, then the possible
   * candidates returned are `a[1].b[3].c[1]`, `a[1].b[4].c[0]`, and
   * `a[2].b[0].c[0]`.
   */
  def incIndices(cPath: CPath): List[CPath] = {

    // zero out the remainin indices in ps.
    @tailrec
    def zero(ps: List[CPathNode], acc: List[CPathNode] = Nil): List[CPathNode] = ps match {
      case CPathIndex(i) :: ps => zero(ps, CPathIndex(0) :: acc)
      case p :: ps => zero(ps, p :: acc)
      case Nil => acc.reverse
    }

    @tailrec
    def cand(left: List[CPathNode], right: List[CPathNode], candidates: List[CPath]): List[CPath] = right match {
      case (p @ CPathIndex(i)) :: right =>
        cand(p :: left, right, CPath(left.foldLeft(CPathIndex(i + 1) :: zero(right)) { (acc, p) =>
          p :: acc
        }) :: candidates)

      case p :: right =>
        cand(p :: left, right, candidates)

      case Nil =>
        candidates
    }

    cand(Nil, cPath.nodes, Nil)
  }
}
