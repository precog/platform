package com.querio.quirrel
package typer

trait CriticalConditionFinder extends parser.AST with Binder {
  import Utils._
  
  override def findCriticalConditions(expr: Expr): Map[String, Set[Expr]] = {
    def loop(root: Let, expr: Expr, currentWhere: Option[Expr]): Map[String, Set[Expr]] = expr match {
      case Let(_, _, _, left, right) => loop(root, right, currentWhere)
      
      case New(_, child) => loop(root, child, currentWhere)
      
      case Relate(_, from, to, in) => {
        val first = loop(root, from, currentWhere)
        val second = loop(root, to, currentWhere)
        val third = loop(root, in, currentWhere)
        merge(merge(first, second), third)
      }
      
      case t @ TicVar(_, id) => t.binding match {
        case UserDef(`root`) => currentWhere map { where => Map(id -> Set(where)) } getOrElse Map()
        case _ => Map()
      }
      
      case StrLit(_, _) => Map()
      case NumLit(_, _) => Map()
      case BoolLit(_, _) => Map()
      
      case ObjectDef(_, props) => {
        val maps = props map { case (_, expr) => loop(root, expr, currentWhere) }
        maps.fold(Map())(merge)
      }
      
      case ArrayDef(_, values) => {
        val maps = values map { expr => loop(root, expr, currentWhere) }
        maps.fold(Map())(merge)
      }
      
      case Descent(_, child, _) => loop(root, child, currentWhere)
      
      case Deref(loc, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case d @ Dispatch(_, _, actuals) => {
        val maps = actuals map { expr => loop(root, expr, currentWhere) }
        val merged = maps.fold(Map())(merge)
        
        val fromDef = d.binding match {
          case UserDef(e) => loop(root, e.left, currentWhere)
          case _ => Map[String, Set[Expr]]()
        }
        
        merge(merged, fromDef)
      }
      
      case Operation(_, left, "where", right) => {
        val leftMap = loop(root, left, currentWhere)
        val rightMap = loop(root, right, Some(right))
        merge(leftMap, rightMap)
      }
      
      case Operation(_, left, _, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case Add(_, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case Sub(_, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case Mul(_, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case Div(_, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case Lt(_, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case LtEq(_, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case Gt(_, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case GtEq(_, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case Eq(_, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case NotEq(_, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case And(_, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case Or(_, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case Comp(_, child) => loop(root, child, currentWhere)
      
      case Neg(_, child) => loop(root, child, currentWhere)
      
      case Paren(_, child) => loop(root, child, currentWhere)
    }
    
    expr match {
      case root @ Let(_, _, _, left, _) => {
        val wheres = loop(root, left, None)
        
        wheres map {
          case (key, value) => key -> (value flatMap splitConj filter referencesTicVar(root))
        }
      }
      case _ => Map()
    }
  }
  
  private def splitConj(expr: Expr): Set[Expr] = expr match {
    case And(_, left, right) => splitConj(left) ++ splitConj(right)
    case e => Set(e)
  }
  
  private def referencesTicVar(root: Let)(expr: Expr): Boolean = expr match {
    case Let(_, _, _, _, right) => referencesTicVar(root)(right)
    
    case New(_, child) => referencesTicVar(root)(child)
    
    case Relate(_, from, to, in) =>
      referencesTicVar(root)(from) || referencesTicVar(root)(to) || referencesTicVar(root)(in)
    
    case t @ TicVar(_, _) => t.binding match {
      case UserDef(`root`) => true
      case _ => false
    }
    
    case StrLit(_, _) | NumLit(_, _) | BoolLit(_, _) => false
    
    case ObjectDef(_, props) => props exists { case (_, e) => referencesTicVar(root)(e) }
    
    case ArrayDef(_, values) => values exists referencesTicVar(root)
    
    case Descent(_, child, _) => referencesTicVar(root)(child)
    
    case Deref(_, left, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)
    
    case d @ Dispatch(_, _, actuals) => {
      val paramRef = actuals exists referencesTicVar(root)
      val defRef = d.binding match {
        case UserDef(e) => referencesTicVar(root)(e.left)
        case _ => false
      }
      
      paramRef || defRef
    }
    
    case Operation(_, left, _, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)
    
    case Add(_, left, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)
    
    case Sub(_, left, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)
    
    case Mul(_, left, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)
    
    case Div(_, left, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)
    
    case Lt(_, left, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)
    
    case LtEq(_, left, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)
    
    case Gt(_, left, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)
    
    case GtEq(_, left, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)
    
    case Eq(_, left, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)
    
    case NotEq(_, left, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)
    
    case And(_, left, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)
    
    case Or(_, left, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)
    
    case Comp(_, child) => referencesTicVar(root)(child)
    
    case Neg(_, child) => referencesTicVar(root)(child)
    
    case Paren(_, child) => referencesTicVar(root)(child)
  }
}
