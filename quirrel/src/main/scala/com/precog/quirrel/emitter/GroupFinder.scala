package com.precog.quirrel
package emitter

trait GroupFinder extends parser.AST with typer.Binder with Solutions {
  import Utils._
  import ast._
  
  override def findGroups(expr: Expr): Map[String, Set[GroupTree]] = {
    import group._
    
    def loop(root: Let, expr: Expr, currentWhere: Option[Where]): Map[String, Set[GroupTree]] = expr match {
      case Let(_, _, _, left, right) => loop(root, right, currentWhere)
      
      case Import(_, _, child) => loop(root, child, currentWhere)
      
      case New(_, child) => loop(root, child, currentWhere)
      
      case Relate(_, from, to, in) => {
        val first = loop(root, from, currentWhere)
        val second = loop(root, to, currentWhere)
        val third = loop(root, in, currentWhere)
        merge(merge(first, second), third)
      }
      
      case t @ TicVar(_, id) => t.binding match {
        case UserDef(`root`) => currentWhere map { where => Map(id -> Set(Condition(where): GroupTree)) } getOrElse Map()
        case _ => Map()
      }
      
      case StrLit(_, _) => Map()
      case NumLit(_, _) => Map()
      case BoolLit(_, _) => Map()
      case NullLit(_) => Map()
      
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
          case _ => Map[String, Set[GroupTree]]()
        }
        
        val back = merge(merged, fromDef)
        
        d.binding match {
          case b: RedLibBuiltIn if d.isReduction =>
            back map { case (key, value) => key -> Set(Reduction(b, value): GroupTree) }
          
          case _ => back
        }
      }
      
      case op @ Where(_, left, right) => {
        val leftMap = loop(root, left, currentWhere)
        val rightMap = loop(root, right, Some(op))
        merge(leftMap, rightMap)
      }
      
      case With(_, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case Union(_, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case Intersect(_, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
            
      case Difference(_, left, right) =>
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
      case root @ Let(_, _, _, left, _) => loop(root, left, None)
      case _ => Map()
    }
  }
  
  
  sealed trait GroupTree
  
  object group {
    case class Condition(op: Where) extends GroupTree
    case class Reduction(b: RedLibBuiltIn, children: Set[GroupTree]) extends GroupTree
  }
}
