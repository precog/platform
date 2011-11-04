package com.reportgrid.quirrel

import edu.uwm.cs.gll.ast.Node
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.collection.parallel.ParSet

trait Solver extends parser.AST {
  import Function._
  
  // VERY IMPORTANT!!!  each rule must represent a monotonic reduction in tree complexity
  private val Rules: Set[PartialFunction[Expr, Set[Expr]]] = Set(
    { case Add(loc, left, right) if left equalsIgnoreLoc right => Set(Mul(loc, NumLit(loc, "2"), left)) },
    { case Sub(loc, left, right) if left equalsIgnoreLoc right => Set(NumLit(loc, "0")) },
    { case Div(loc, left, right) if left equalsIgnoreLoc right => Set(NumLit(loc, "1")) },
    
    { case Add(loc, left, right) => Set(Add(loc, right, left)) },
    { case Sub(loc, left, right) => Set(Add(loc, Neg(loc, right), left)) },
    { case Add(loc, Neg(_, left), right) => Set(Sub(loc, right, left)) },
    { case Mul(loc, left, right) => Set(Mul(loc, right, left)) },
    
    { case Add(loc, Mul(loc2, x, y), z) if y equalsIgnoreLoc z => Set(Mul(loc2, Add(loc, x, NumLit(loc, "1")), y)) },
    { case Add(loc, Mul(loc2, w, x), Mul(loc3, y, z)) if x equalsIgnoreLoc z => Set(Mul(loc2, Add(loc, w, y), x)) },
    
    { case Add(loc, Div(loc2, x, y), z) => Set(Div(loc2, Add(loc, x, Mul(loc, z, y)), y)) },
    { case Add(loc, Div(loc2, w, x), Div(loc3, y, z)) => Set(Div(loc2, Add(loc, Mul(loc2, w, z), Mul(loc3, y, x)), Mul(loc2, x, z))) },
    
    { case Neg(loc, Div(loc2, x, y)) => Set(Div(loc2, Neg(loc, x), y), Div(loc2, x, Neg(loc, y))) },
    { case Neg(loc, Mul(loc2, x, y)) => Set(Mul(loc2, Neg(loc, x), y), Mul(loc2, x, Neg(loc, y))) },
  
    { case Add(loc, left, right) => for (left2 <- possibilities(left) + left; right2 <- possibilities(right) + right) yield Add(loc, left2, right2) },
    { case Sub(loc, left, right) => for (left2 <- possibilities(left) + left; right2 <- possibilities(right) + right) yield Sub(loc, left2, right2) },
    { case Mul(loc, left, right) => for (left2 <- possibilities(left) + left; right2 <- possibilities(right) + right) yield Mul(loc, left2, right2) },
    { case Div(loc, left, right) => for (left2 <- possibilities(left) + left; right2 <- possibilities(right) + right) yield Div(loc, left2, right2) },
    
    { case Neg(loc, child) => possibilities(child) map neg(loc) },
    { case Paren(_, child) => Set(child) })
    
  private[this] val enableTrace = false
  
  def solve(tree: Expr)(predicate: PartialFunction[Node, Boolean]): Expr => Option[Expr] = tree match {
    case n if predicate.isDefinedAt(n) && predicate(n) => Some apply _
    case Add(loc, left, right) => solveBinary(tree, left, right, predicate)(sub(loc), sub(loc))
    case Sub(loc, left, right) => solveBinary(tree, left, right, predicate)(add(loc), sub(loc) andThen { _ andThen neg(loc) })
    case Mul(loc, left, right) => solveBinary(tree, left, right, predicate)(div(loc), div(loc))
    case Div(loc, left, right) => solveBinary(tree, left, right, predicate)(mul(loc), flip(div(loc)))
    case Neg(loc, child) => solve(child)(predicate) andThen { _ map neg(loc) }
    case Paren(_, child) => solve(child)(predicate)
    case _ => const(None) _
  }
  
  private def solveBinary(tree: Expr, left: Expr, right: Expr, predicate: PartialFunction[Node, Boolean])(invertLeft: Expr => Expr => Expr, invertRight: Expr => Expr => Expr): Expr => Option[Expr] = {
    val totalPred = predicate.lift andThen { _ getOrElse false }
    val inLeft = isSubtree(totalPred)(left)
    val inRight = isSubtree(totalPred)(right)
    
    if (inLeft && inRight) {
      val results = simplify(tree, totalPred) map { xs =>
        (solve(xs.head)(predicate), xs)
      }
      
      results.foldLeft(const[Option[Expr], Expr](None) _) { 
        case (acc, (f, trace)) => e =>
          acc(e) orElse (f(e) map { e2 => printTrace(trace); e2 })
      }
    } else if (inLeft && !inRight) {
      solve(left)(predicate) compose flip(invertLeft)(right)
    } else if (!inLeft && inRight) {
      solve(right)(predicate) compose flip(invertRight)(left)
    } else {
      const(None)
    }
  }
  
  def simplify(tree: Expr, predicate: Node => Boolean) =
    search(predicate, Set(tree :: Nil), Set(), Set())
  
  @tailrec
  private[this] def search(predicate: Node => Boolean, work: Set[List[Expr]], seen: Set[Expr], results: Set[List[Expr]]): Set[List[Expr]] = {
    val filteredWork = work filterNot { xs => seen(xs.head) }
    // println("Examining: " + (filteredWork map { e => "\n" + printSExp(e) }))
    if (filteredWork.isEmpty) {
      results
    } else {
      val (results2, newWork) = filteredWork partition { xs => isSimplified(predicate)(xs.head) }
      val newWorkLists = newWork flatMap { xs =>
        possibilities(xs.head) map { _ :: xs }
      }
      search(predicate, newWorkLists, seen ++ (filteredWork map { _.head }), results ++ results2)
    }
  }
  
  private def isSimplified(predicate: Node => Boolean)(tree: Expr) = tree match {
    case Add(_, left, right) => !isSubtree(predicate)(left) || !isSubtree(predicate)(right)
    case Sub(_, left, right) => !isSubtree(predicate)(left) || !isSubtree(predicate)(right)
    case Mul(_, left, right) => !isSubtree(predicate)(left) || !isSubtree(predicate)(right)
    case Div(_, left, right) => !isSubtree(predicate)(left) || !isSubtree(predicate)(right)
    case _ => true
  }
  
  def possibilities(expr: Expr): Set[Expr] =
    Rules filter { _ isDefinedAt expr } flatMap { _(expr) }
  
  def isSubtree(predicate: Node => Boolean)(tree: Node): Boolean =
    predicate(tree) || (tree.children map isSubtree(predicate) exists identity)
  
  def printTrace(trace: List[Expr]) {
    if (enableTrace) {
      println("*** Solution Point!")
      println(trace.reverse map { e => printSExp(e) } mkString "\n\n")
    }
  }
  
  private val add = curried(Add)
  private val sub = curried(Sub)
  private val mul = curried(Mul)
  private val div = curried(Div)
  private val neg = curried(Neg)
  
  private def flip[A, B, C](f: A => B => C)(b: B)(a: A) = f(a)(b)
}
