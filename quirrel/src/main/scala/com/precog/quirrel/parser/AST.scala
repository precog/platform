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
package com.precog
package quirrel
package parser

import util.Atom
import util.BitSet

import com.codecommit.gll.LineStream
import com.codecommit.gll.ast._

import scalaz.Scalaz._
import scalaz.Tree

trait AST extends Phases {
  import Atom._
  import ast._
  
  type Solution
  
  type BucketSpec

  type NameBinding
  type VarBinding
  
  type Provenance
  type ProvConstraint

  def printSExp(tree: Expr, indent: String = ""): String = tree match {
    case Add(_, left, right) => "%s(+\n%s\n%s)".format(indent, printSExp(left, indent + "  "), printSExp(right, indent + "  "))
    case Sub(_, left, right) => "%s(-\n%s\n%s)".format(indent, printSExp(left, indent + "  "), printSExp(right, indent + "  "))
    case Mul(_, left, right) => "%s(*\n%s\n%s)".format(indent, printSExp(left, indent + "  "), printSExp(right, indent + "  "))
    case Div(_, left, right) => "%s(/\n%s\n%s)".format(indent, printSExp(left, indent + "  "), printSExp(right, indent + "  "))
    case Neg(_, child) => "%s(neg\n%s)".format(indent, printSExp(child, indent + "  "))
    case Paren(_, child) => printSExp(child, indent)
    case NumLit(_, value) => indent + value
    case TicVar(_, id) => indent + id
    case _ => indent + "<unprintable>"
  }
  
  def printInfix(tree: Expr): String = tree match {
    case Add(_, left, right) => "(%s + %s)".format(printInfix(left), printInfix(right))
    case Sub(_, left, right) => "(%s - %s)".format(printInfix(left), printInfix(right))
    case Mul(_, left, right) => "(%s * %s)".format(printInfix(left), printInfix(right))
    case Div(_, left, right) => "(%s / %s)".format(printInfix(left), printInfix(right))
    case Neg(_, child) => "neg%s".format(printInfix(child))
    case Paren(_, child) => "(%s)".format(printInfix(child))
    case NumLit(_, value) => value
    case TicVar(_, id) => id
    case _ => "<unprintable>"
  }
  
  def prettyPrint(e: Expr, level: Int = 0): String = {
    val indent = 0 until level map Function.const(' ') mkString
    
    val back = e match {
      case e @ Let(loc, id, params, left, right) => {
        val paramStr = params map { indent + "  - " + _ } mkString "\n"
        
        indent + "type: let\n" +
          indent + "id: " + id + "\n" +
          indent + "params:\n" + paramStr + "\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }

      case Solve(loc, constraints, child) => {
        val constraintsStr = constraints map { indent + "  -\n" + prettyPrint(_, level + 4) } mkString "\n"
        
        indent + "type: solve\n" +
          indent + "constraints:\n" + constraintsStr + "\n" +
          indent + "child:\n" + prettyPrint(child, level + 2)
      }
      
      case Import(loc, spec, child) => {
        val specStr = spec match {
          case WildcardImport(prefix) => prefix mkString ("", "::", "::_")
          case SpecificImport(prefix) => prefix mkString "::"
        }
        
        indent + "type: import\n" +
          indent + "spec: " + specStr +
          indent + "child: \n" + prettyPrint(child, level + 2)
      }
      
      case Assert(loc, pred, child) => {
        indent + "type: assert\n" +
          indent + "pred: " + prettyPrint(pred, level + 2) +
          indent + "child: \n" + prettyPrint(child, level + 2)
      }
      
      case Observe(loc, data, samples) => {
        indent + "type: observe\n" +
          indent + "data: " + prettyPrint(data, level + 2) +
          indent + "samples: \n" + prettyPrint(samples, level + 2)
      }
      
      case New(loc, child) => {
        indent + "type: new\n" +
          indent + "child:\n" + prettyPrint(child, level + 2)
      }
      
      case Relate(loc, from: Expr, to: Expr, in: Expr) => {
        indent + "type: \n" +
          indent + "from:\n" + prettyPrint(from, level + 2) + "\n" +
          indent + "to:\n" + prettyPrint(to, level + 2) + "\n" +
          indent + "in:\n" + prettyPrint(in, level + 2)
      }
      
      case t @ TicVar(loc, id) => {
        indent + "type: ticvar\n" +
          indent + "id: " + id + "\n" +
          indent + "binding: " + t.binding.toString
      }
      
      case StrLit(loc, value) => {
        indent + "type: str\n" +
          indent + "value: " + value
      }
      
      case NumLit(loc, value) => {
        indent + "type: num\n" +
          indent + "value: " + value
      }
      
      case BoolLit(loc, value) => {
        indent + "type: bool\n" +
          indent + "value: " + value
      }
            
      case UndefinedLit(loc) => {
        indent + "type: undefined\n"
      }

      case NullLit(loc) => {
        indent + "type: null\n" 
      }
      
      case ObjectDef(loc, props) => {
        val propStr = props map {
          case (name, value) => {
            indent + "  - \n" +
              indent + "    name: " + name + "\n" +
              indent + "    value:\n" + prettyPrint(value, level + 6)
          }
        } mkString "\n"
        
        indent + "type: object\n" +
          indent + "properties:\n" + propStr
      }
      
      case ArrayDef(loc, values) => {
        val valStr = values map { indent + "  -\n" + prettyPrint(_, level + 4) } mkString "\n"
        
        indent + "type: array\n" +
          indent + "values:\n" + valStr
      }
      
      case Descent(loc, child, property) => {
        indent + "type: descent\n" +
          indent + "child:\n" + prettyPrint(child, level + 2) + "\n" +
          indent + "property: " + property
      }
      
      case MetaDescent(loc, child, property) => {
        indent + "type: meta-descent\n" +
          indent + "child:\n" + prettyPrint(child, level + 2) + "\n" +
          indent + "property: " + property
      }
      
      case Deref(loc, left, right) => {
        indent + "type: deref\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n"
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case d @ Dispatch(loc, name, actuals) => {
        val actualsStr = actuals map { indent + "  -\n" + prettyPrint(_, level + 4) } mkString "\n"
        
        indent + "type: dispatch\n" +
          indent + "name: " + name + "\n" +
          indent + "actuals:\n" + actualsStr + "\n" +
          indent + "binding: " + d.binding.toString + "\n" +
          indent + "is-reduction: " + d.isReduction
      }

      case Cond(loc, pred, left, right) =>
        indent + "type: cond\n" +
          indent + "pred:\n" + prettyPrint(pred, level + 2) + "\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)

      case Where(loc, left, right) => {
        indent + "type: where\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }

      case With(loc, left, right) => {
        indent + "type: with\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }

      case Union(loc, left, right) => {
        indent + "type: union\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }

      case Intersect(loc, left, right) => {
        indent + "type: intersect\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }     
      
      case Difference(loc, left, right) => {
        indent + "type: without\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case Add(loc, left, right) => {
        indent + "type: add\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case Sub(loc, left, right) => {
        indent + "type: sub\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case Mul(loc, left, right) => {
        indent + "type: mul\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case Div(loc, left, right) => {
        indent + "type: div\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }      

      case Mod(loc, left, right) => {
        indent + "type: mod\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case Pow(loc, left, right) => {
        indent + "type: pow\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }

      case Lt(loc, left, right) => {
        indent + "type: lt\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case LtEq(loc, left, right) => {
        indent + "type: LtEq\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case Gt(loc, left, right) => {
        indent + "type: gt\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case GtEq(loc, left, right) => {
        indent + "type: GtEq\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case Eq(loc, left, right) => {
        indent + "type: eq\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case NotEq(loc, left, right) => {
        indent + "type: noteq\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case Or(loc, left, right) => {
        indent + "type: or\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case And(loc, left, right) => {
        indent + "type: and\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case Comp(loc, child) => {
        indent + "type: comp\n" +
          indent + "child:\n" + prettyPrint(child, level + 2)
      }
      
      case Neg(loc, child) => {
        indent + "type: neg\n" +
          indent + "child:\n" + prettyPrint(child, level + 2)
      }
      
      case Paren(loc, child) => {
        indent + "type: paren\n" +
          indent + "child:\n" + prettyPrint(child, level + 2)
      }
    }
    
    val constraintStr = e.constrainingExpr map { e =>
      "\n" + indent + "constraining-expr: @" + e.nodeId
    } getOrElse ""
    
    indent + "nodeId: " + e.nodeId + "\n" +
      indent + "line: " + e.loc.lineNum + "\n" +
      indent + "col: " + e.loc.colNum + "\n" +
      back + "\n" +
      indent + "provenance: " + e.provenance.toString +
      constraintStr
  }
  
  protected def bindRoot(root: Expr, e: Expr) {
    def bindElements(a: Any) {
      a match {
        case e: Expr => bindRoot(root, e)
        
        case (e1: Expr, e2: Expr) => {
          bindRoot(root, e1)
          bindRoot(root, e2)
        }
        
        case (e: Expr, _) => bindRoot(root, e)
        case (_, e: Expr) => bindRoot(root, e)
        
        case _ =>
      }
    }
  
    e.root = root
    
    e.productIterator foreach {
      case e: Expr => bindRoot(root, e)
      case v: Vector[_] => v foreach bindElements
      case _ =>
    }
  }

  sealed trait Expr extends Node with Product { self =>
    val nodeId = System.identityHashCode(this)
    
    private val _root = atom[Expr]
    def root = _root()
    private[AST] def root_=(e: Expr) = _root() = e
    
    private val _provenance = attribute[Provenance](checkProvenance)
    def provenance = _provenance()
    private[quirrel] def provenance_=(p: Provenance) = _provenance() = p
    
    private val _constrainingExpr = attribute[Option[Expr]](checkProvenance)
    def constrainingExpr = _constrainingExpr()
    private[quirrel] def constrainingExpr_=(expr: Option[Expr]) = _constrainingExpr() = expr

    private val _relations = attribute[Map[Provenance, Set[Provenance]]](checkProvenance)
    def relations = _relations()
    private[quirrel] def relations_=(rels: Map[Provenance, Set[Provenance]]) = _relations() = rels
    
    private[quirrel] final lazy val _errors: Atom[Set[Error]] = {
      if (this eq root) {
        atom[Set[Error]] {
          _errors ++= runPhasesInSequence(root)
        }
      } else {
        val back = root._errors
        6 * 7     // do not remove!  SI-5455
        back
      }
    }
    
    final def errors = _errors()
    
    def loc: LineStream

    def disallowsInfinite: Boolean = false

    override def children: List[Expr]

    //todo consider another data structure besides `scalaz.Tree`
    private lazy val subForest: Stream[Tree[Expr]] = {
      def subForest0(l: List[Expr]): Stream[Tree[Expr]] = l match {
        case Nil => Stream.empty

        case head :: tail => Stream.cons(head.tree, subForest0(tail))
      }

      subForest0(children)
    }

    //todo consider another data structure besides `scalaz.Tree`
    def tree: Tree[Expr] = Tree.node(this, subForest)
    
    /**
     * Returns a trace for the ''entire'' tree from the root (not just
     * this specific sub-expression).  Tracing is not really well-defined
     * for sub-expressions sans-context, so this semantic makes some sense.
     * It would arguably be better to build the trace from the root and then
     * drill down to find this particular sub-expression.  That becomes
     * problematic though as sub-expressions may appear multiple times in a
     * single root trace.
     */
    lazy val trace: Trace = {
      if (this eq root) {
        buildTrace(Map())(root)
      } else {
        val back = root.trace
        6 * 7         // SI-5455
        back
      }
    }
    
    override def toString: String = {
      val result = productIterator map {
        case ls: LineStream => "<%d:%d>".format(ls.lineNum, ls.colNum)
        case x => x.toString
      }
      
      productPrefix + "(%s)".format(result mkString ",")
    }
    
    def equalsIgnoreLoc(that: Expr): Boolean = (this, that) match {
      case (a, b) if a == b => true
      
      case (Let(_, id1, params1, left1, right1), Let(_, id2, params2, left2, right2)) =>
        (id1 == id2) &&
          (params1 == params2) &&
          (left1 equalsIgnoreLoc left2) &&
          (right1 equalsIgnoreLoc right2)

      case (Solve(_, constraints1, child1), Solve(_, constraints2, child2)) => {
        val sizing = constraints1.length == constraints2.length
        val contents = constraints1 zip constraints2 forall {
          case (e1, e2) => e1 equalsIgnoreLoc e2
        }
        
        sizing && contents && (child1 equalsIgnoreLoc child2)
      }
      
      case (Import(_, spec1, child1), Import(_, spec2, child2)) =>
        (child1 equalsIgnoreLoc child2) && (spec1 == spec2)
      
      case (Assert(_, pred1, child1), Assert(_, pred2, child2)) =>
        (child1 equalsIgnoreLoc child2) && (pred1 equalsIgnoreLoc pred2)

      case (Observe(_, data1, samples1), Observe(_, data2, samples2)) =>
        (data1 equalsIgnoreLoc data2) && (samples1 equalsIgnoreLoc samples2)

      case (New(_, child1), New(_, child2)) =>
        child1 equalsIgnoreLoc child2

      case (Relate(_, from1, to1, in1), Relate(_, from2, to2, in2)) =>
        (from1 equalsIgnoreLoc from2) &&
          (to1 equalsIgnoreLoc to2) &&
          (in1 equalsIgnoreLoc in2)

      case (TicVar(_, id1), TicVar(_, id2)) =>
        id1 == id2


      case (StrLit(_, value1), StrLit(_, value2)) =>
        value1 == value2

      case (NumLit(_, value1), NumLit(_, value2)) =>
        value1 == value2

      case (BoolLit(_, value1), BoolLit(_, value2)) =>
        value1 == value2

      case (UndefinedLit(_), UndefinedLit(_)) =>
        true

      case (NullLit(_), NullLit(_)) =>
        true

      case (ObjectDef(_, props1), ObjectDef(_, props2)) => {      // TODO ordering
        val sizing = props1.length == props2.length
        val contents = props1 zip props2 forall {
          case ((key1, value1), (key2, value2)) =>
            (key1 == key2) && (value1 equalsIgnoreLoc value2)
        }

        sizing && contents
      }

      case (ArrayDef(_, values1), ArrayDef(_, values2)) => {
        val sizing = values1.length == values2.length
        val contents = values1 zip values2 forall {
          case (e1, e2) => e1 equalsIgnoreLoc e2
        }

        sizing && contents
      }

      case (Descent(_, child1, property1), Descent(_, child2, property2)) =>
        (child1 equalsIgnoreLoc child2) && (property1 == property2)

      case (MetaDescent(_, child1, property1), MetaDescent(_, child2, property2)) =>
        (child1 equalsIgnoreLoc child2) && (property1 == property2)

      case (Deref(_, left1, right1), Deref(_, left2, right2)) =>
        (left1 equalsIgnoreLoc left2) && (right1 equalsIgnoreLoc right2)

      case (d1 @ Dispatch(_, name1, actuals1), d2 @ Dispatch(_, name2, actuals2)) => {
        val naming = name1 == name2
        val sizing = actuals1.length == actuals2.length
        val binding = d1.binding == d2.binding
        val contents = actuals1 zip actuals2 forall {
          case (e1, e2) => e1 equalsIgnoreLoc e2
        }

        naming && sizing && binding && contents
      }

      case (Cond(_, pred1, left1, right1), Cond(_, pred2, left2, right2)) =>
        (pred1 equalsIgnoreLoc pred2) && (left1 equalsIgnoreLoc left2) && (right1 equalsIgnoreLoc right2)

      case (Where(_, left1, right1), Where(_, left2, right2)) => 
        (left1 equalsIgnoreLoc left2) && (right1 equalsIgnoreLoc right2)
      
      case (With(_, left1, right1), With(_, left2, right2)) => 
        (left1 equalsIgnoreLoc left2) && (right1 equalsIgnoreLoc right2)
      
      case (Union(_, left1, right1), Union(_, left2, right2)) => 
        (left1 equalsIgnoreLoc left2) && (right1 equalsIgnoreLoc right2)
      
      case (Intersect(_, left1, right1), Intersect(_, left2, right2)) => 
        (left1 equalsIgnoreLoc left2) && (right1 equalsIgnoreLoc right2)
         
      case (Difference(_, left1, right1), Difference(_, left2, right2)) => 
        (left1 equalsIgnoreLoc left2) && (right1 equalsIgnoreLoc right2)

      case (Add(_, left1, right1), Add(_, left2, right2)) =>
        (left1 equalsIgnoreLoc left2) && (right1 equalsIgnoreLoc right2)
      
      case (Sub(_, left1, right1), Sub(_, left2, right2)) =>
        (left1 equalsIgnoreLoc left2) && (right1 equalsIgnoreLoc right2)
      
      case (Mul(_, left1, right1), Mul(_, left2, right2)) =>
        (left1 equalsIgnoreLoc left2) && (right1 equalsIgnoreLoc right2)
      
      case (Div(_, left1, right1), Div(_, left2, right2)) =>
        (left1 equalsIgnoreLoc left2) && (right1 equalsIgnoreLoc right2)
      
      case (Mod(_, left1, right1), Mod(_, left2, right2)) =>
        (left1 equalsIgnoreLoc left2) && (right1 equalsIgnoreLoc right2)
      
      case (Lt(_, left1, right1), Lt(_, left2, right2)) =>
        (left1 equalsIgnoreLoc left2) && (right1 equalsIgnoreLoc right2)
      
      case (LtEq(_, left1, right1), LtEq(_, left2, right2)) =>
        (left1 equalsIgnoreLoc left2) && (right1 equalsIgnoreLoc right2)
      
      case (Gt(_, left1, right1), Gt(_, left2, right2)) =>
        (left1 equalsIgnoreLoc left2) && (right1 equalsIgnoreLoc right2)
      
      case (GtEq(_, left1, right1), GtEq(_, left2, right2)) =>
        (left1 equalsIgnoreLoc left2) && (right1 equalsIgnoreLoc right2)
      
      case (Eq(_, left1, right1), Eq(_, left2, right2)) =>
        (left1 equalsIgnoreLoc left2) && (right1 equalsIgnoreLoc right2)
      
      case (NotEq(_, left1, right1), NotEq(_, left2, right2)) =>
        (left1 equalsIgnoreLoc left2) && (right1 equalsIgnoreLoc right2)
      
      case (And(_, left1, right1), And(_, left2, right2)) =>
        (left1 equalsIgnoreLoc left2) && (right1 equalsIgnoreLoc right2)
      
      case (Or(_, left1, right1), Or(_, left2, right2)) =>
        (left1 equalsIgnoreLoc left2) && (right1 equalsIgnoreLoc right2)
      
      case (Comp(_, child1), Comp(_, child2)) => child1 equalsIgnoreLoc child2

      case (Neg(_, child1), Neg(_, child2)) => child1 equalsIgnoreLoc child2

      case (Paren(_, child1), Paren(_, child2)) => child1 equalsIgnoreLoc child2
      
      case _ => false
    }

    def hashCodeIgnoreLoc: Int = this match {
      case Let(_, id, params, left, right) =>
        id.hashCode + params.hashCode + left.hashCodeIgnoreLoc + right.hashCodeIgnoreLoc

      case Solve(_, constraints, child) =>
        (constraints map { _.hashCodeIgnoreLoc } sum) + child.hashCodeIgnoreLoc
      
      case Import(_, spec, child) =>
        spec.hashCode + child.hashCodeIgnoreLoc
      
      case Assert(_, pred, child) =>
        pred.hashCodeIgnoreLoc + child.hashCodeIgnoreLoc

      case Observe(_, data, samples) =>
        data.hashCodeIgnoreLoc + samples.hashCodeIgnoreLoc

      case New(_, child) => child.hashCodeIgnoreLoc * 23

      case Relate(_, from, to, in) =>
        from.hashCodeIgnoreLoc + to.hashCodeIgnoreLoc + in.hashCodeIgnoreLoc

      case TicVar(_, id) => id.hashCode

      case StrLit(_, value) => value.hashCode

      case NumLit(_, value) => value.hashCode

      case BoolLit(_, value) => value.hashCode

      case UndefinedLit(_) => "undefined".hashCode

      case NullLit(_) => "null".hashCode

      case ObjectDef(_, props) => {
        props map {
          case (key, value) => key.hashCode + value.hashCodeIgnoreLoc
        } sum
      }

      case ArrayDef(_, values) =>
        values map { _.hashCodeIgnoreLoc } sum

      case Descent(_, child, property) =>
        child.hashCodeIgnoreLoc + property.hashCode

      case MetaDescent(_, child, property) =>
        child.hashCodeIgnoreLoc + property.hashCode

      case Deref(_, left, right) =>
        left.hashCodeIgnoreLoc + right.hashCodeIgnoreLoc

      case d @ Dispatch(_, name, actuals) =>
        name.hashCode + d.binding.hashCode + (actuals map { _.hashCodeIgnoreLoc } sum)

      case Cond(_, pred, left, right) =>
        "if".hashCode + pred.hashCodeIgnoreLoc + "then".hashCode + left.hashCodeIgnoreLoc + "else".hashCode + right.hashCodeIgnoreLoc

      case Where(_, left, right) =>
        left.hashCodeIgnoreLoc + "where".hashCode + right.hashCodeIgnoreLoc

      case With(_, left, right) =>
        left.hashCodeIgnoreLoc + "with".hashCode + right.hashCodeIgnoreLoc

      case Union(_, left, right) =>
        left.hashCodeIgnoreLoc + "union".hashCode + right.hashCodeIgnoreLoc

      case Intersect(_, left, right) =>
        left.hashCodeIgnoreLoc + "intersect".hashCode + right.hashCodeIgnoreLoc

      case Difference(_, left, right) =>
        left.hashCodeIgnoreLoc + "without".hashCode + right.hashCodeIgnoreLoc

      case Add(_, left, right) =>
        left.hashCodeIgnoreLoc + right.hashCodeIgnoreLoc

      case Sub(_, left, right) =>
        left.hashCodeIgnoreLoc + right.hashCodeIgnoreLoc

      case Mul(_, left, right) =>
        left.hashCodeIgnoreLoc + right.hashCodeIgnoreLoc

      case Div(_, left, right) =>
        left.hashCodeIgnoreLoc + right.hashCodeIgnoreLoc

      case Mod(_, left, right) =>
        left.hashCodeIgnoreLoc + right.hashCodeIgnoreLoc

      case Pow(_, left, right) =>
        left.hashCodeIgnoreLoc + right.hashCodeIgnoreLoc

      case Lt(_, left, right) =>
        left.hashCodeIgnoreLoc + right.hashCodeIgnoreLoc

      case LtEq(_, left, right) =>
        left.hashCodeIgnoreLoc + right.hashCodeIgnoreLoc

      case Gt(_, left, right) =>
        left.hashCodeIgnoreLoc + right.hashCodeIgnoreLoc

      case GtEq(_, left, right) =>
        left.hashCodeIgnoreLoc + right.hashCodeIgnoreLoc

      case Eq(_, left, right) =>
        left.hashCodeIgnoreLoc + right.hashCodeIgnoreLoc

      case NotEq(_, left, right) =>
        left.hashCodeIgnoreLoc + right.hashCodeIgnoreLoc

      case And(_, left, right) =>
        left.hashCodeIgnoreLoc + right.hashCodeIgnoreLoc

      case Or(_, left, right) =>
        left.hashCodeIgnoreLoc + right.hashCodeIgnoreLoc

      case Comp(_, child) => child.hashCodeIgnoreLoc * 13

      case Neg(_, child) => child.hashCodeIgnoreLoc * 7

      case Paren(_, child) => child.hashCodeIgnoreLoc * 29
    }
    
    protected def attribute[A](phase: Phase): Atom[A] = atom[A] {
      _errors ++= phase(root)
    }
  }
  
  private[quirrel] case class ExprWrapper(expr: Expr) {
    override def equals(a: Any): Boolean = a match {
      case ExprWrapper(expr2) => expr equalsIgnoreLoc expr2
      case _ => false
    }

    override def hashCode = expr.hashCodeIgnoreLoc
  }
  
  object ast {
    /*
     * The `Precedence*` traits exist only for the purpose of abstracting over
     * various AST nodes in the precedence/associativity disambiguation framework.
     * They should *not* be used for attributes mixed into multiple AST nodes.
     */
     
    sealed trait PrecedenceLeafNode extends Expr with LeafNode
    
    sealed trait PrecedenceBinaryNode extends Expr with BinaryNode {
      override def left: Expr
      override def right: Expr
      
      def assocLeft = true

      override def children = List(left, right)
    }
    
    sealed trait PrecedenceUnaryNode extends Expr with UnaryNode {
      override def child: Expr

      override def children = List(child)
    }
    
    /*
     * Overlapping ADT instances
     */
    
    sealed trait NaryOp extends Expr {
      def values: Vector[Expr]
    }
    
    object NaryOp {
      def unapply(op: NaryOp): Some[(LineStream, Vector[Expr])] =
        Some((op.loc, op.values))
    }
     
    sealed trait Literal extends Expr with NaryOp with PrecedenceLeafNode {
      def values = Vector()
    }
    
    object Literal {
      def unapply(lit: Literal): Some[LineStream] = Some(lit.loc)
    }
    
    sealed trait UnaryOp extends Expr with NaryOp {
      def child: Expr
      def values = Vector(child)
    }
    
    object UnaryOp {
      def unapply(un: UnaryOp): Some[(LineStream, Expr)] =
        Some((un.loc, un.child))
    }
    
    sealed trait BinaryOp extends Expr with NaryOp with PrecedenceBinaryNode {
      def values = Vector(left, right)
    }
    
    object BinaryOp {
      def unapply(bin: BinaryOp): Some[(LineStream, Expr, Expr)] =
        Some((bin.loc, bin.left, bin.right))
    }
    
    sealed trait ComparisonOp extends BinaryOp
    
    object ComparisonOp {
      def unapply(bin: ComparisonOp): Some[(LineStream, Expr, Expr)] =
        Some((bin.loc, bin.left, bin.right))
    }

    /*
     * Raw AST nodes
     */
    
    final case class Let(loc: LineStream, name: Identifier, params: Vector[String], left: Expr, right: Expr) extends PrecedenceUnaryNode {
      val sym = 'let
      
      val isPrefix = true
      
      def child = right
      
      private val _dispatches = attribute[Set[Dispatch]](bindNames)
      def dispatches = _dispatches()
      private[quirrel] def dispatches_=(dispatches: Set[Dispatch]) = _dispatches() = dispatches
      private[quirrel] def dispatches_+=(dispatch: Dispatch) = _dispatches += dispatch
      private[quirrel] def dispatches_++=(dispatches: Set[Dispatch]) = _dispatches ++= dispatches
      
      lazy val substitutions: Map[Dispatch, Map[String, Expr]] = {
        dispatches.map({ dispatch =>
          dispatch -> Map(params zip dispatch.actuals: _*)
        })(collection.breakOut)
      }
      
      private val _constraints = attribute[Set[ProvConstraint]](checkProvenance)
      def constraints = _constraints()
      private[quirrel] def constraints_=(constraints: Set[ProvConstraint]) = _constraints() = constraints
      
      private val _resultProvenance = attribute[Provenance](checkProvenance)
      def resultProvenance = _resultProvenance()
      private[quirrel] def resultProvenance_=(prov: Provenance) = _resultProvenance() = prov
    }

    final case class Solve(loc: LineStream, constraints: Vector[Expr], child: Expr) extends Expr with Node {
      val sym = 'solve
      
      def form = 'solve ~ (constraints.init map { _ ~ 'comma } reduceOption { _ ~ _ } map { _ ~ constraints.last ~ child } getOrElse (constraints.last ~ child))
      
      def children = child +: constraints toList
      
      private val _vars = attribute[Set[TicId]](bindNames)
      def vars: Set[TicId] = _vars()
      private[quirrel] def vars_=(vars: Set[TicId]) = _vars() = vars
      
      lazy val criticalConstraints = constraints filter {
        case TicVar(_, _) => false
        case _ => true
      } toSet
      
      private val _buckets = attribute[Map[Set[Dispatch], BucketSpec]](inferBuckets)
      def buckets = _buckets()
      private[quirrel] def buckets_=(spec: Map[Set[Dispatch], BucketSpec]) = _buckets() = spec
      private[quirrel] def buckets_+=(spec: (Set[Dispatch], BucketSpec)) = _buckets += spec
      private[quirrel] def buckets_++=(spec: Map[Set[Dispatch], BucketSpec]) = _buckets ++= spec
    }

    final case class Import(loc: LineStream, spec: ImportSpec, child: Expr) extends Expr with UnaryOp with PrecedenceUnaryNode {
      val sym = 'import
      val isPrefix = true
    }

    final case class Assert(loc: LineStream, pred: Expr, child: Expr) extends Expr {
      val sym = 'assert
      
      def form = 'assert ~ pred ~ child
      
      def children = List(pred, child)
    }
    
    final case class Observe(loc: LineStream, data: Expr, samples: Expr) extends Expr {
      val sym = 'observe
      def left = data
      def right = samples

      def children = List(data, samples)

      def form = 'observe ~ 'leftParen ~ data ~ 'comma ~ samples ~ 'rightParen
    }

    final case class New(loc: LineStream, child: Expr) extends Expr with PrecedenceUnaryNode {
      val sym = 'new
      val isPrefix = true
    }
    
    final case class Relate(loc: LineStream, from: Expr, to: Expr, in: Expr) extends Expr with Node {
      val sym = 'relate
      
      def form = from ~ 'relate ~ to ~ in
      
      override def children = List(in, to, from)
    }
    
    final case class TicVar(loc: LineStream, name: TicId) extends PrecedenceLeafNode {
      val sym = 'ticvar
      
      private val _binding = attribute[VarBinding](bindNames)
      def binding = _binding()
      private[quirrel] def binding_=(b: VarBinding) = _binding() = b
    }
    
    final case class StrLit(loc: LineStream, value: String) extends Literal {
      val sym = 'str
    }
    
    final case class NumLit(loc: LineStream, value: String) extends Literal {
      val sym = 'num
    }
    
    final case class BoolLit(loc: LineStream, value: Boolean) extends Literal {
      val sym = 'bool
    }
    
    final case class UndefinedLit(loc: LineStream) extends Literal {
      val sym = 'undefined
    }

    final case class NullLit(loc: LineStream) extends Literal {
      val sym = 'null
    }
    
    final case class ObjectDef(loc: LineStream, props: Vector[(String, Expr)]) extends Expr with NaryOp {
      val sym = 'object
      
      def values = props map { _._2 }
      
      def form = {
        val opt = (props map { case (_, e) => 'name ~ e } reduceOption { _ ~ _ })
        
        opt map { 'leftCurl ~ _ ~ 'rightCurl } getOrElse sym 
      }
      
      def children = values.toList
    }
    
    final case class ArrayDef(loc: LineStream, values: Vector[Expr]) extends Expr with NaryOp {
      val sym = 'array
      
      def form = {
        val opt = (values map { _ ~ 'comma } reduceOption { _ ~ _ })
        
        opt map { 'leftBracket ~ _ ~ 'rightBracket } getOrElse sym 
      }
      
      def children = values.toList
    }
    
    final case class Descent(loc: LineStream, child: Expr, property: String) extends Expr with UnaryOp with PrecedenceUnaryNode {
      val sym = 'descent
      val isPrefix = false
    }
    
    final case class MetaDescent(loc: LineStream, child: Expr, property: String) extends Expr with UnaryOp with PrecedenceUnaryNode {
      val sym = 'metaDescent
      val isPrefix = false
    }
    
    final case class Deref(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryOp {
      val sym = 'deref
      
      override def form = left ~ 'leftBracket ~ right ~ 'rightBracket
    }

    final case class Dispatch(loc: LineStream, name: Identifier, actuals: Vector[Expr]) extends Expr with NaryOp {
      val sym = 'dispatch
      
      def values = actuals
      
      private val _isReduction = attribute[Boolean](bindNames)
      def isReduction = _isReduction()
      private[quirrel] def isReduction_=(b: Boolean) = _isReduction() = b
      
      private val _binding = attribute[NameBinding](bindNames)
      def binding = _binding()
      private[quirrel] def binding_=(b: NameBinding) = _binding() = b
      
      def form = {
        val opt = (actuals map { _ ~ 'comma } reduceOption { _ ~ _ })
        
        opt map { sym ~ 'leftParen ~ _ ~ 'rightParen } getOrElse sym
      }
      
      def children = actuals.toList
    }
    
    final case class Cond(loc: LineStream, pred: Expr, left: Expr, right: Expr) extends Expr with NaryOp {
      val sym = 'cond
      
      def values = Vector(pred, left, right)

      def form = 'if ~ pred ~ 'then ~ left ~ 'else ~ right

      def children = values.toList
    }

    final case class Where(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryOp {
      val sym = 'where
      override val disallowsInfinite = true
    }

    final case class With(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryOp {
      val sym = 'with
      override val disallowsInfinite = true
    }
    
    final case class Union(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryOp {
      val sym = 'union
    }

    final case class Intersect(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryOp {
      val sym = 'intersect
    }

    final case class Difference(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryOp {
      val sym = 'difference
    }

    final case class Add(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryOp {
      val sym = 'add
    }
    
    final case class Sub(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryOp {
      val sym = 'sub
    }
    
    final case class Mul(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryOp {
      val sym = 'mul
    }
    
    final case class Div(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryOp {
      val sym = 'div
    }
    
    final case class Mod(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryOp {
      val sym = 'mod
    }
    
    final case class Pow(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryOp {
      val sym = 'pow
    }

    final case class Lt(loc: LineStream, left: Expr, right: Expr) extends Expr with ComparisonOp {
      val sym = 'lt
    }
    
    final case class LtEq(loc: LineStream, left: Expr, right: Expr) extends Expr with ComparisonOp {
      val sym = 'lteq
    }
    
    final case class Gt(loc: LineStream, left: Expr, right: Expr) extends Expr with ComparisonOp {
      val sym = 'gt
    }
    
    final case class GtEq(loc: LineStream, left: Expr, right: Expr) extends Expr with ComparisonOp {
      val sym = 'gteq
    }
    
    final case class Eq(loc: LineStream, left: Expr, right: Expr) extends Expr with ComparisonOp {
      val sym = 'eq
      override val disallowsInfinite = true
    }
    
    final case class NotEq(loc: LineStream, left: Expr, right: Expr) extends Expr with ComparisonOp {
      val sym = 'noteq
      override val disallowsInfinite = true
    }
    
    final case class And(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryOp {
      val sym = 'and
      override val disallowsInfinite = true
    }
    
    final case class Or(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryOp {
      val sym = 'or
      override val disallowsInfinite = true
    }
    
    final case class Comp(loc: LineStream, child: Expr) extends Expr with UnaryOp with PrecedenceUnaryNode {
      val sym = 'comp
      val isPrefix = true
    }
    
    final case class Neg(loc: LineStream, child: Expr) extends Expr with UnaryOp with PrecedenceUnaryNode {
      val sym = 'neg
      val isPrefix = true
    }
    
    final case class Paren(loc: LineStream, child: Expr) extends Expr with UnaryOp {
      val sym = 'paren
      def form = 'leftParen ~ child ~ 'rightParen
      def children = child :: Nil
    }
   
    
    sealed trait ImportSpec
    final case class SpecificImport(prefix: Vector[String]) extends ImportSpec
    final case class WildcardImport(prefix: Vector[String]) extends ImportSpec
  }
}
