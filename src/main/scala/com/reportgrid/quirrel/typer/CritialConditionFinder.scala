package com.reportgrid.quirrel
package typer

trait CriticalConditionFinder extends parser.AST with Binder {
  import Utils._
  
  override def findCriticalConditions(expr: Expr): Map[String, Set[Expr]] = {
    def loop(root: Let, expr: Expr, currentWhere: Option[Expr]): Map[String, Set[Expr]] = expr match {
      case Let(_, _, _, left, right) => 
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
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
      
      case Dispatch(_, _, actuals) => {
        val maps = actuals map { expr => loop(root, expr, currentWhere) }
        maps.fold(Map())(merge)
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
      case root @ Let(_, _, _, left, _) => loop(root, left, None)
      case _ => Map()
    }
  }
}
