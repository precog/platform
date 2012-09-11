package com.precog.quirrel
package emitter

trait GroupFinder extends parser.AST with typer.Binder with typer.ProvenanceChecker with Solutions {
  import Utils._
  import ast._
  
  override def findGroups(expr: Expr): Set[GroupTree] = {
    import group._
    
    def loop(root: Solve, expr: Expr, currentWhere: Option[Where]): Set[GroupTree] = expr match {
      case Let(_, _, _, left, right) => loop(root, right, currentWhere)

      case Import(_, _, child) => loop(root, child, currentWhere)
      
      case New(_, child) => loop(root, child, currentWhere)
      
      case Relate(_, from, to, in) => {
        val first = loop(root, from, currentWhere)
        val second = loop(root, to, currentWhere)
        val third = loop(root, in, currentWhere)
        first ++ second
      }
      
      case t @ TicVar(_, id) => t.binding match {
        case SolveBinding(`root`) =>
          currentWhere map { where => Set(GroupCondition(where): GroupTree) } getOrElse Set()
        
        case _ => Set()
      }
      
      case StrLit(_, _) => Set()
      case NumLit(_, _) => Set()
      case BoolLit(_, _) => Set()
      case NullLit(_) => Set()
      
      case ObjectDef(_, props) => {
        val sets = props map { case (_, expr) => loop(root, expr, currentWhere) }
        sets.fold(Set()) { _ ++ _ }
      }
      
      case ArrayDef(_, values) => {
        val sets = values map { expr => loop(root, expr, currentWhere) }
        sets.fold(Set()) { _ ++ _ }
      }
      
      case Descent(_, child, _) => loop(root, child, currentWhere)
      
      case Deref(loc, left, right) =>
        loop(root, left, currentWhere) ++ loop(root, right, currentWhere)
      
      case d @ Dispatch(_, _, actuals) => {
        val sets = actuals map { expr => loop(root, expr, currentWhere) }
        val merged = sets.fold(Set()) { _ ++ _ }
        
        val fromDef = d.binding match {
          case LetBinding(e) => loop(root, e.left, currentWhere)
          case _ => Set[GroupTree]()
        }
        
        merged ++ fromDef
      }
      
      case op @ Where(_, left, right) => {
        val leftSet = loop(root, left, currentWhere)
        val rightSet = loop(root, right, Some(op))
        
        leftSet ++ rightSet
      }
      
      case With(_, left, right) =>
        loop(root, left, currentWhere) ++ loop(root, right, currentWhere)
      
      case Union(_, left, right) =>
        loop(root, left, currentWhere) ++ loop(root, right, currentWhere)
      
      case Intersect(_, left, right) =>
        loop(root, left, currentWhere) ++ loop(root, right, currentWhere)
            
      case Difference(_, left, right) =>
        loop(root, left, currentWhere) ++ loop(root, right, currentWhere)
      
      case Add(_, left, right) =>
        loop(root, left, currentWhere) ++ loop(root, right, currentWhere)
      
      case Sub(_, left, right) =>
        loop(root, left, currentWhere) ++ loop(root, right, currentWhere)
      
      case Mul(_, left, right) =>
        loop(root, left, currentWhere) ++ loop(root, right, currentWhere)
      
      case Div(_, left, right) =>
        loop(root, left, currentWhere) ++ loop(root, right, currentWhere)
      
      case Lt(_, left, right) =>
        loop(root, left, currentWhere) ++ loop(root, right, currentWhere)
      
      case LtEq(_, left, right) =>
        loop(root, left, currentWhere) ++ loop(root, right, currentWhere)
      
      case Gt(_, left, right) =>
        loop(root, left, currentWhere) ++ loop(root, right, currentWhere)
      
      case GtEq(_, left, right) =>
        loop(root, left, currentWhere) ++ loop(root, right, currentWhere)
      
      case Eq(_, left, right) =>
        loop(root, left, currentWhere) ++ loop(root, right, currentWhere)
      
      case NotEq(_, left, right) =>
        loop(root, left, currentWhere) ++ loop(root, right, currentWhere)
      
      case And(_, left, right) =>
        loop(root, left, currentWhere) ++ loop(root, right, currentWhere)
      
      case Or(_, left, right) =>
        loop(root, left, currentWhere) ++ loop(root, right, currentWhere)
      
      case Comp(_, child) => loop(root, child, currentWhere)
      
      case Neg(_, child) => loop(root, child, currentWhere)
      
      case Paren(_, child) => loop(root, child, currentWhere)
    }
    
    expr match {
      case root @ Solve(_, constraints, child) => {
        val filtered = constraints filter { 
          case TicVar(_, _) => false
          case _ => true
        }

        val groupTrees = filtered map GroupConstraint toSet

        groupTrees ++ loop(root, child, None)
      }
      
      case _ => Set()
    }
  }

  
  
  sealed trait GroupTree
  
  object group {
    case class GroupCondition(op: Where) extends GroupTree
    case class GroupConstraint(constr: Expr) extends GroupTree
  }
}
