package com.reportgrid.quirrel

import edu.uwm.cs.gll.ast.Node
import scala.annotation.tailrec
import scala.collection.parallel.ParSet

trait Solver extends parser.AST {
  import Function._
  
  // VERY IMPORTANT!!!  each rule must represent a monotonic reduction in tree complexity
  private val Rules: ParSet[PartialFunction[Expr, Set[Expr]]] = ParSet()
  
  def solve(tree: Expr, node: Expr): Expr => Option[Expr] = tree match {
    case `node` => Some apply _
    case Add(loc, left, right) => solveBinary(tree, left, right, node)(Sub(loc, _, _))
  }
  
  private def solveBinary(tree: Expr, left: Expr, right: Expr, node: Expr)(invert: (Expr, Expr) => Expr): Expr => Option[Expr] = {
    val inLeft = isSubtree(node)(left)
    val inRight = isSubtree(node)(right)
    
    if (inLeft && inRight) {
      val results = simplify(tree, node) map { solve(_, node) }
      
      results.foldLeft(const[Option[Expr], Expr](None) _) { (acc, f) => e =>
        acc(e) orElse f(e)
      }
    } else if (inLeft && !inRight) {
      val adjust = invert(_: Expr, right)
      solve(left, node) andThen { _ map adjust }
    } else if (!inLeft && inRight) {
      val adjust = invert(_: Expr, left)
      solve(left, node) andThen { _ map adjust }
    } else {
      const(None)
    }
  }
  
  private def simplify(tree: Expr, node: Expr) =
    search(node, ParSet(tree), ParSet(), ParSet()).seq
  
  @tailrec
  private[this] def search(node: Expr, work: ParSet[Expr], seen: ParSet[Expr], results: ParSet[Expr]): ParSet[Expr] = {
    val filteredWork = work &~ seen
    if (filteredWork.isEmpty) {
      results
    } else {
      val (results2, newWork) = filteredWork partition isSimplified(node)
      search(node, newWork flatMap possibilities, seen ++ filteredWork, results ++ results2)
    }
  }
  
  private def isSimplified(node: Expr)(tree: Expr) = false
  
  private def possibilities(expr: Expr): ParSet[Expr] =
    Rules filter { _ isDefinedAt expr } flatMap { _(expr) }
  
  def isSubtree(node: Node)(tree: Node): Boolean =
    tree == node || (tree.children map isSubtree(node) exists identity)
}
