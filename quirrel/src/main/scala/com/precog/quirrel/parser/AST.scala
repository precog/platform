package com.precog
package quirrel
package parser

import util.Atom

import com.codecommit.gll.LineStream
import com.codecommit.gll.ast._

import scalaz.Scalaz._
import scalaz.Tree

trait AST extends Phases {
  import Atom._
  import ast._
  
  type Solution
  
  type Bucket

  type Binding
  type FormalBinding
  type Provenance
  
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
        
        val assumptionStr = e.assumptions map {
          case (name, prov) => {
            indent + "  -\n" +
              indent + "    name: " + name + "\n" +
              indent + "    provenance: " + prov.toString
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

      override def children: List[Expr]

      private lazy val subForest: Stream[Tree[Expr]] = {
        def subForest0(l: List[Expr]): Stream[Tree[Expr]] = l match {
          case Nil => Stream.empty

          case head :: tail => Stream.cons(head.tree, subForest0(tail))
        }

        subForest0(children)
      }

      def tree: Tree[Expr] = Tree.node(this, subForest)
      
      def equalsIgnoreLoc(that: Expr): Boolean = (this, that) match {
        case (Let(_, id1, params1, left1, right1), Let(_, id2, params2, left2, right2)) =>
          (id1 == id2) &&
            (params1 == params2) &&
            (left1 equalsIgnoreLoc left2) &&
            (right1 equalsIgnoreLoc right2)

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

        case (Deref(_, left1, right1), Deref(_, left2, right2)) =>
          (left1 equalsIgnoreLoc left2) && (right1 equalsIgnoreLoc right2)

        case (Dispatch(_, name1, actuals1), Dispatch(_, name2, actuals2)) => {
          val naming = name1 == name2
          val sizing = actuals1.length == actuals2.length
          val contents = actuals1 zip actuals2 forall {
            case (e1, e2) => e1 equalsIgnoreLoc e2
          }

          naming && sizing && contents
        }

        case (Where(_, left1, right1), Where(_, left2, right2)) => {
          (left1 equalsIgnoreLoc left2) &&
            (right1 equalsIgnoreLoc right2)
        }
        case (With(_, left1, right1), Where(_, left2, right2)) => {
          (left1 equalsIgnoreLoc left2) &&
            (right1 equalsIgnoreLoc right2)
        }
        case (Union(_, left1, right1), Union(_, left2, right2)) => {
          (left1 equalsIgnoreLoc left2) &&
            (right1 equalsIgnoreLoc right2)
        }
        case (Intersect(_, left1, right1), Intersect(_, left2, right2)) => {
          (left1 equalsIgnoreLoc left2) &&
            (right1 equalsIgnoreLoc right2)
        }

        case (Add(_, left1, right1), Add(_, left2, right2)) =>
          (left1 equalsIgnoreLoc left2) && (right1 equalsIgnoreLoc right2)
        
        case (Sub(_, left1, right1), Sub(_, left2, right2)) =>
          (left1 equalsIgnoreLoc left2) && (right1 equalsIgnoreLoc right2)
        
        case (Mul(_, left1, right1), Mul(_, left2, right2)) =>
          (left1 equalsIgnoreLoc left2) && (right1 equalsIgnoreLoc right2)
        
        case (Div(_, left1, right1), Div(_, left2, right2)) =>
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

        case New(_, child) => child.hashCodeIgnoreLoc * 23

        case Relate(_, from, to, in) =>
          from.hashCodeIgnoreLoc + to.hashCodeIgnoreLoc + in.hashCodeIgnoreLoc

        case TicVar(_, id) => id.hashCode

        case StrLit(_, value) => value.hashCode

        case NumLit(_, value) => value.hashCode

        case BoolLit(_, value) => value.hashCode

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

        case Deref(_, left, right) =>
          left.hashCodeIgnoreLoc + right.hashCodeIgnoreLoc

        case Dispatch(_, name, actuals) =>
          name.hashCode + (actuals map { _.hashCodeIgnoreLoc } sum)

        case Where(_, left, right) =>
          left.hashCodeIgnoreLoc + "where".hashCode + right.hashCodeIgnoreLoc

        case With(_, left, right) =>
          left.hashCodeIgnoreLoc + "with".hashCode + right.hashCodeIgnoreLoc

        case Union(_, left, right) =>
          left.hashCodeIgnoreLoc + "union".hashCode + right.hashCodeIgnoreLoc

        case Intersect(_, left, right) =>
          left.hashCodeIgnoreLoc + "intersect".hashCode + right.hashCodeIgnoreLoc

        case Add(_, left, right) =>
          left.hashCodeIgnoreLoc + right.hashCodeIgnoreLoc

        case Sub(_, left, right) =>
          left.hashCodeIgnoreLoc + right.hashCodeIgnoreLoc

        case Mul(_, left, right) =>
          left.hashCodeIgnoreLoc + right.hashCodeIgnoreLoc

        case Div(_, left, right) =>
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
 
  object ast {    
    sealed trait ExprLeafNode extends Expr with LeafNode
    
    sealed trait ExprBinaryNode extends Expr with BinaryNode {
      override def left: Expr
      override def right: Expr

      override def children = List(left, right)
    }
    
    sealed trait RelationExpr extends ExprBinaryNode
    
    sealed trait ExprUnaryNode extends Expr with UnaryNode {
      override def child: Expr

      override def children = List(child)
    }

    final case class Let(loc: LineStream, name: Identifier, params: Vector[TicId], left: Expr, right: Expr) extends ExprBinaryNode {
      val label = 'let
      
      lazy val criticalConditions = findCriticalConditions(this)
      lazy val groups = findGroups(this)
      
      private val _assumptions = attribute[Map[String, Provenance]](checkProvenance)
      def assumptions = _assumptions()
      private[quirrel] def assumptions_=(map: Map[String, Provenance]) = _assumptions() = map
      
      private val _unconstrainedParams = attribute[Set[String]](checkProvenance)
      def unconstrainedParams = _unconstrainedParams()
      private[quirrel] def unconstrainedParams_=(up: Set[String]) = _unconstrainedParams() = up
      
      private val _requiredParams = attribute[Int](checkProvenance)
      def requiredParams = _requiredParams()
      private[quirrel] def requiredParams_=(req: Int) = _requiredParams() = req
    }
    
    final case class New(loc: LineStream, child: Expr) extends ExprUnaryNode {
      val label = 'new
      val isPrefix = true
    }
    
    final case class Relate(loc: LineStream, from: Expr, to: Expr, in: Expr) extends ExprBinaryNode {
      val label = 'relate
      
      val left = from
      val right = to
      override def children = List(from, to, in)
    }
    
    final case class TicVar(loc: LineStream, name: TicId) extends ExprLeafNode {
      val label = 'ticvar
      
      private val _binding = attribute[FormalBinding](bindNames)
      def binding = _binding()
      private[quirrel] def binding_=(b: FormalBinding) = _binding() = b
    }
    
    final case class StrLit(loc: LineStream, value: String) extends ExprLeafNode {
      val label = 'str
    }
    
    final case class NumLit(loc: LineStream, value: String) extends ExprLeafNode {
      val label = 'num
    }
    
    final case class BoolLit(loc: LineStream, value: Boolean) extends ExprLeafNode {
      val label = 'bool
    }
    
    final case class NullLit(loc: LineStream) extends ExprLeafNode {
      val label = 'null
    }
    
    final case class ObjectDef(loc: LineStream, props: Vector[(String, Expr)]) extends Expr {
      val label = 'object
      
      def children = props map { _._2 } toList
    }
    
    final case class ArrayDef(loc: LineStream, values: Vector[Expr]) extends Expr {
      val label = 'array
      
      def children = values.toList
    }
    
    final case class Descent(loc: LineStream, child: Expr, property: String) extends ExprUnaryNode {
      val label = 'descent
      val isPrefix = false
    }
    
    final case class Deref(loc: LineStream, left: Expr, right: Expr) extends ExprUnaryNode {
      val label = 'deref
      val isPrefix = true
      val child = left
    }

    final case class Dispatch(loc: LineStream, name: Identifier, actuals: Vector[Expr]) extends Expr {
      val label = 'dispatch
      
      private val _isReduction = attribute[Boolean](bindNames)
      def isReduction = _isReduction()
      private[quirrel] def isReduction_=(b: Boolean) = _isReduction() = b
      
      private val _binding = attribute[Binding](bindNames)
      def binding = _binding()
      private[quirrel] def binding_=(b: Binding) = _binding() = b
      
      private val _equalitySolutions = attribute[Map[String, Solution]](solveCriticalConditions)
      def equalitySolutions = _equalitySolutions()
      private[quirrel] def equalitySolutions_=(map: Map[String, Solution]) = _equalitySolutions() = map
      
      private val _buckets = attribute[Map[String, Bucket]](inferBuckets)
      def buckets = _buckets()
      private[quirrel] def buckets_=(map: Map[String, Bucket]) = _buckets() = map
      
      def children = actuals.toList
    }
    
    final case class Where(loc: LineStream, left: Expr, right: Expr) extends ExprBinaryNode {
      val label = 'where
    }

    final case class With(loc: LineStream, left: Expr, right: Expr) extends ExprBinaryNode {
      val label = 'with
    }
    
    final case class Union(loc: LineStream, left: Expr, right: Expr) extends ExprBinaryNode {
      val label = 'union
    }

    final case class Intersect(loc: LineStream, left: Expr, right: Expr) extends ExprBinaryNode {
      val label = 'intersect
    }

    final case class Add(loc: LineStream, left: Expr, right: Expr) extends ExprBinaryNode {
      val label = 'add
    }
    
    final case class Sub(loc: LineStream, left: Expr, right: Expr) extends ExprBinaryNode {
      val label = 'sub
    }
    
    final case class Mul(loc: LineStream, left: Expr, right: Expr) extends ExprBinaryNode {
      val label = 'mul
    }
    
    final case class Div(loc: LineStream, left: Expr, right: Expr) extends ExprBinaryNode {
      val label = 'div
    }
    
    final case class Lt(loc: LineStream, left: Expr, right: Expr) extends RelationExpr {
      val label = 'lt
    }
    
    final case class LtEq(loc: LineStream, left: Expr, right: Expr) extends RelationExpr {
      val label = 'lteq
    }
    
    final case class Gt(loc: LineStream, left: Expr, right: Expr) extends RelationExpr {
      val label = 'gt
    }
    
    final case class GtEq(loc: LineStream, left: Expr, right: Expr) extends RelationExpr {
      val label = 'gteq
    }
    
    final case class Eq(loc: LineStream, left: Expr, right: Expr) extends RelationExpr {
      val label = 'eq
    }
    
    final case class NotEq(loc: LineStream, left: Expr, right: Expr) extends RelationExpr {
      val label = 'noteq
    }
    
    final case class And(loc: LineStream, left: Expr, right: Expr) extends ExprBinaryNode {
      val label = 'and
    }
    
    final case class Or(loc: LineStream, left: Expr, right: Expr) extends ExprBinaryNode {
      val label = 'or
    }
    
    final case class Comp(loc: LineStream, child: Expr) extends ExprUnaryNode {
      val label = 'comp
      val isPrefix = true
    }
    
    final case class Neg(loc: LineStream, child: Expr) extends ExprUnaryNode {
      val label = 'neg
      val isPrefix = true
    }
    
    final case class Paren(loc: LineStream, child: Expr) extends Expr {
      val label = 'paren
      val children = child :: Nil
    }
  }
}
