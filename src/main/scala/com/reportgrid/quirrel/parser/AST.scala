package com.reportgrid.quirrel
package parser

import com.reportgrid.quirrel.util.{Atom, SetAtom}
import edu.uwm.cs.gll.LineStream
import edu.uwm.cs.gll.ast._

trait AST extends Phases {
  import Atom._
  
  type Binding
  type FormalBinding
  type Provenance
  
  def printSExp(tree: Expr, indent: String = ""): String = tree match {
    case Add(_, left, right) => "%s(+\n%s\n%s)".format(indent, printSExp(left, indent + "  "), printSExp(right, indent + "  "))
    case Sub(_, left, right) => "%s(-\n%s\n%s)".format(indent, printSExp(left, indent + "  "), printSExp(right, indent + "  "))
    case Mul(_, left, right) => "%s(*\n%s\n%s)".format(indent, printSExp(left, indent + "  "), printSExp(right, indent + "  "))
    case Div(_, left, right) => "%s(/\n%s\n%s)".format(indent, printSExp(left, indent + "  "), printSExp(right, indent + "  "))
    case Neg(_, child) => "%s(~\n%s)".format(indent, printSExp(child, indent + "  "))
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
    case Neg(_, child) => "~%s".format(printInfix(child))
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
        
        val assumptionStr = e.assumptions map {
          case (name, prov) => {
            indent + "  -\n" +
              indent + "    name: " + name + "\n" +
              indent + "    provenance:\n" + prov.toString
          }
        } mkString "\n"
        
        val unconstrainedStr = e.unconstrainedParams map { name =>
          indent + "  - " + name
        } mkString "\n"
        
        indent + "type: let\n" +
          indent + "id: " + id + "\n" +
          indent + "params:\n" + paramStr + "\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2) + "\n" +
          indent + "assumptions:\n" + assumptionStr + "\n" +
          indent + "unconstrained-params:\n" + unconstrainedStr + "\n" +
          indent + "required-params: " + e.requiredParams
      }
      
      case New(loc, child) => {
        indent + "type: new\n" +
          indent + "child:\n" + prettyPrint(child, level + 2)
      }
      
      case Relate(loc, from: Expr, to: Expr, in: Expr) => {
        indent + "type: relate\n" +
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
      
      case Operation(loc, left, op, right) => {
        indent + "type: op\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "op: " + op + "\n" +
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
    
    indent + "nodeId: " + e.nodeId + "\n" +
      indent + "line: " + e.loc.lineNum + "\n" +
      indent + "col: " + e.loc.colNum + "\n" +
      back + "\n" +
      indent + "provenance: " + e.provenance.toString
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
  
    e._root() = root
    
    e.productIterator foreach {
      case e: Expr => bindRoot(root, e)
      case v: Vector[_] => v foreach bindElements
      case _ =>
    }
  }
  
  sealed trait Expr extends Node with Product {
    val nodeId = System.identityHashCode(this)
    
    private[parser] val _root = atom[Expr]
    def root = _root()
    
    private[quirrel] val _provenance = attribute[Provenance](checkProvenance)
    def provenance = _provenance()
    
    private[quirrel] final lazy val _errors: SetAtom[Error] =
      if (this eq root) new SetAtom[Error] else root._errors
    
    final def errors = _errors()
    
    def loc: LineStream
    
    def equalsIgnoreLoc(that: Expr): Boolean = (this, that) match {
      case (Add(_, left1, right1), Add(_, left2, right2)) =>
        (left1 equalsIgnoreLoc left2) && (right1 equalsIgnoreLoc right2)
      
      case (Sub(_, left1, right1), Sub(_, left2, right2)) =>
        (left1 equalsIgnoreLoc left2) && (right1 equalsIgnoreLoc right2)
      
      case (Mul(_, left1, right1), Mul(_, left2, right2)) =>
        (left1 equalsIgnoreLoc left2) && (right1 equalsIgnoreLoc right2)
      
      case (Div(_, left1, right1), Div(_, left2, right2)) =>
        (left1 equalsIgnoreLoc left2) && (right1 equalsIgnoreLoc right2)
      
      case (Neg(_, child1), Neg(_, child2)) => child1 equalsIgnoreLoc child2
      case (Paren(_, child1), Paren(_, child2)) => child1 equalsIgnoreLoc child2
      
      case (TicVar(_, id1), TicVar(_, id2)) => id1 == id2
      
      case _ => false
    }
    
    protected def attribute[A](phase: Phase): Atom[A] = atom[A] {
      _errors ++= phase(root)
    }
  }
  
  case class Let(loc: LineStream, id: String, params: Vector[String], left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'let
    
    lazy val criticalConditions = findCriticalConditions(this)
    
    private[quirrel] val _assumptions = attribute[Map[String, Provenance]](checkProvenance)
    def assumptions = _assumptions()
    
    private[quirrel] val _unconstrainedParams = attribute[Set[String]](checkProvenance)
    def unconstrainedParams = _unconstrainedParams()
    
    private[quirrel] val _requiredParams = attribute[Int](checkProvenance)
    def requiredParams = _requiredParams()
  }
  
  case class New(loc: LineStream, child: Expr) extends Expr with UnaryNode {
    val label = 'new
    val isPrefix = true
  }
  
  case class Relate(loc: LineStream, from: Expr, to: Expr, in: Expr) extends Expr with BinaryNode {
    val label = 'relate
    
    val left = from
    val right = to
    override def children = List(from, to, in)
  }
  
  case class TicVar(loc: LineStream, id: String) extends Expr with LeafNode {
    val label = 'ticvar
    
    private[quirrel] val _binding = attribute[FormalBinding](bindNames)
    def binding = _binding()
  }
  
  case class StrLit(loc: LineStream, value: String) extends Expr with LeafNode {
    val label = 'str
  }
  
  case class NumLit(loc: LineStream, value: String) extends Expr with LeafNode {
    val label = 'num
  }
  
  case class BoolLit(loc: LineStream, value: Boolean) extends Expr with LeafNode {
    val label = 'bool
  }
  
  case class ObjectDef(loc: LineStream, props: Vector[(String, Expr)]) extends Expr {
    val label = 'object
    
    def children = props map { _._2 } toList
  }
  
  case class ArrayDef(loc: LineStream, values: Vector[Expr]) extends Expr {
    val label = 'array
    
    def children = values.toList
  }
  
  case class Descent(loc: LineStream, child: Expr, property: String) extends Expr with UnaryNode {
    val label = 'descent
    val isPrefix = false
  }
  
  case class Deref(loc: LineStream, left: Expr, right: Expr) extends Expr with UnaryNode {
    val label = 'deref
    val isPrefix = true
    val child = left
  }
  
  case class Dispatch(loc: LineStream, name: String, actuals: Vector[Expr]) extends Expr {
    val label = 'dispatch
    
    private[quirrel] val _isReduction = attribute[Boolean](bindNames)
    def isReduction = _isReduction()
    
    private[quirrel] val _binding = attribute[Binding](bindNames)
    def binding = _binding()
    
    def children = actuals.toList
  }
  
  case class Operation(loc: LineStream, left: Expr, op: String, right: Expr) extends Expr with BinaryNode {
    val label = if (op == "where") 'where else 'op
  }
  
  case class Add(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'add
  }
  
  case class Sub(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'sub
  }
  
  case class Mul(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'mul
  }
  
  case class Div(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'div
  }
  
  case class Lt(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'lt
  }
  
  case class LtEq(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'lteq
  }
  
  case class Gt(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'gt
  }
  
  case class GtEq(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'gteq
  }
  
  case class Eq(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'eq
  }
  
  case class NotEq(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'noteq
  }
  
  case class And(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'and
  }
  
  case class Or(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'or
  }
  
  case class Comp(loc: LineStream, child: Expr) extends Expr with UnaryNode {
    val label = 'comp
    val isPrefix = true
  }
  
  case class Neg(loc: LineStream, child: Expr) extends Expr with UnaryNode {
    val label = 'neg
    val isPrefix = true
  }
  
  case class Paren(loc: LineStream, child: Expr) extends Expr {
    val label = 'paren
    val children = child :: Nil
  }
}
