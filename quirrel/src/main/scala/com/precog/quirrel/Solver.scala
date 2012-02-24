package com.precog.quirrel

import edu.uwm.cs.gll.ast.Node
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.collection.parallel.ParSet

trait Solver extends parser.AST {
  import Function._
  import ast._
  
  // VERY IMPORTANT!!!  each rule must represent a monotonic reduction in tree complexity
  private val Rules: Set[PartialFunction[Expr, Set[Expr]]] = Set(
    { case Add(loc, left, right) if left equalsIgnoreLoc right => Set(Mul(loc, NumLit(loc, "2"), left)) },
    { case Sub(loc, left, right) if left equalsIgnoreLoc right => Set(NumLit(loc, "0")) },
    { case Div(loc, left, right) if left equalsIgnoreLoc right => Set(NumLit(loc, "1")) },
    
    { case Add(loc, left, right) => Set(Add(loc, right, left)) },
    { case Sub(loc, left, right) => Set(Add(loc, Neg(loc, right), left)) },
    { case Add(loc, Neg(_, left), right) => Set(Sub(loc, right, left)) },
    { case Mul(loc, left, right) => Set(Mul(loc, right, left)) },
    
    { case Add(loc, Add(loc2, x, y), z) => Set(Add(loc, x, Add(loc2, y, z))) },
    { case Mul(loc, Mul(loc2, x, y), z) => Set(Mul(loc, x, Mul(loc2, y, z))) },
    
    { case Add(loc, Mul(loc2, x, y), z) if y equalsIgnoreLoc z => Set(Mul(loc2, Add(loc, x, NumLit(loc, "1")), y)) },
    { case Add(loc, Mul(loc2, w, x), Mul(loc3, y, z)) if x equalsIgnoreLoc z => Set(Mul(loc2, Add(loc, w, y), x)) },
    
    { case Sub(loc, Mul(loc2, x, y), z) if y equalsIgnoreLoc z => Set(Mul(loc2, Sub(loc, x, NumLit(loc, "1")), y)) },
    { case Sub(loc, Mul(loc2, w, x), Mul(loc3, y, z)) if x equalsIgnoreLoc z => Set(Mul(loc2, Sub(loc, w, y), x)) },
    
    { case Add(loc, Div(loc2, x, y), z) => Set(Div(loc2, Add(loc, x, Mul(loc, z, y)), y)) },
    { case Add(loc, Div(loc2, w, x), Div(loc3, y, z)) => Set(Div(loc2, Add(loc, Mul(loc2, w, z), Mul(loc3, y, x)), Mul(loc2, x, z))) },
    
    { case Mul(loc, Div(loc2, x, y), z) => Set(Div(loc2, Mul(loc, x, z), y)) },
    { case Mul(loc, Div(loc2, w, x), Div(loc3, y, z)) => Set(Div(loc2, Mul(loc, w, y), Mul(loc, x, z))) },
    
    { case Div(_, Mul(_, x, y), z) if x equalsIgnoreLoc z => Set(y) },
    { case Div(loc, Mul(_, w, x), Mul(_, y, z)) if w equalsIgnoreLoc y => Set(Div(loc, x, z)) },
    
    { case Div(loc, Div(loc2, w, x), Div(loc3, y, z)) => Set(Div(loc, Mul(loc2, w, z), Mul(loc3, x, y))) },
    
    { case Sub(loc, Div(loc2, x, y), z) => Set(Div(loc2, Sub(loc, x, Mul(loc, z, y)), y)) },
    { case Sub(loc, Div(loc2, w, x), Div(loc3, y, z)) => Set(Div(loc2, Sub(loc, Mul(loc2, w, z), Mul(loc3, y, x)), Mul(loc2, x, z))) },
    
    { case Neg(loc, Add(loc2, x, y)) => Set(Add(loc2, Neg(loc, x), Neg(loc, y))) },
    { case Neg(loc, Sub(loc2, x, y)) => Set(Sub(loc2, Neg(loc, x), Neg(loc, y))) },
    { case Neg(loc, Mul(loc2, x, y)) => Set(Mul(loc2, Neg(loc, x), y), Mul(loc2, x, Neg(loc, y))) },
    { case Neg(loc, Div(loc2, x, y)) => Set(Div(loc2, Neg(loc, x), y), Div(loc2, x, Neg(loc, y))) },
    { case Neg(_, Neg(_, x)) => Set(x) },
    
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

  /**
   * Note that this really only works for `Eq` at present.  Will improve things
   * further in future.
   */
  def solveRelation(re: RelationExpr)(predicate: PartialFunction[Node, Boolean]): Option[Expr] = {
    val leftRight = re match {
      case Lt(_, _, _) => None
      case LtEq(_, _, _) => None
      case Gt(_, _, _) => None
      case GtEq(_, _, _) => None
      
      case Eq(loc, left, right) => Some((loc, left, right))
      case NotEq(_, _, _) => None
    }
    
    val result = leftRight flatMap {
      case (loc, left, right) if predicate.isDefinedAt(left) && predicate(left) =>
        Some(right)
      
      case (loc, left, right) if predicate.isDefinedAt(right) && predicate(right) =>
        Some(left)
      
      case (loc, left, right) => {
        // try both addition and multiplication groups
        lazy val sub = Sub(loc, left, right)
        lazy val div = Div(loc, left, right)
        
        lazy val first = solve(sub)(predicate)(NumLit(loc, "0"))
        lazy val second = solve(div)(predicate)(NumLit(loc, "1"))
        
        first orElse second
      }
    }
    
    // big assumption here!!!!  we're assuming that these phases only require synthetic attributes
    result foreach runPhasesInSequence
    
    result
  }
  
  def solveComplement(c: Comp): PartialFunction[Node, Boolean] => Option[Expr] = c.child match {
    case Lt(loc, left, right) => solveRelation(GtEq(loc, left, right))
    case LtEq(loc, left, right) => solveRelation(Gt(loc, left, right))
    case Gt(loc, left, right) => solveRelation(LtEq(loc, left, right))
    case GtEq(loc, left, right) => solveRelation(Lt(loc, left, right))
    
    case Eq(loc, left, right) => solveRelation(NotEq(loc, left, right))
    case NotEq(loc, left, right) => solveRelation(Eq(loc, left, right))
    
    case _ => const(None)
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
    // println("Examining: " + (filteredWork map { _.head } map printInfix))
    if (filteredWork.isEmpty) {
      results
    } else {
      val (results2, newWork) = filteredWork partition { xs => isSimplified(predicate)(xs.head) }
      val newWorkLists = newWork flatMap { xs =>
        possibilities(xs.head) map { _ :: xs }
      }
      
      // return just the first set of results we find
      if (results2.isEmpty)
        search(predicate, newWorkLists, seen ++ (filteredWork map { _.head }), results ++ results2)
      else
        results2
    }
  }
  
  private def isSimplified(predicate: Node => Boolean)(tree: Expr) = tree match {
    case Add(_, left, right) => !isSubtree(predicate)(left) || !isSubtree(predicate)(right)
    case Sub(_, left, right) => !isSubtree(predicate)(left) || !isSubtree(predicate)(right)
    case Mul(_, left, right) => !isSubtree(predicate)(left) || !isSubtree(predicate)(right)
    case Div(_, left, right) => !isSubtree(predicate)(left) || !isSubtree(predicate)(right)
    case _ => !isSubtree(predicate)(tree)
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
  
  private val add = Add.curried
  private val sub = Sub.curried
  private val mul = Mul.curried
  private val div = Div.curried
  private val neg = Neg.curried
  
  private def flip[A, B, C](f: A => B => C)(b: B)(a: A) = f(a)(b)
}
