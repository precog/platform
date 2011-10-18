package com.reportgrid.quirrel
package typer

trait ProvenanceChecker extends parser.AST with Binder {
  
  override def checkProvenance(expr: Expr) = {
    val Message = "cannot perform operation on unrelated sets"
    
    def loop(expr: Expr, relations: Set[(Provenance, Provenance)]): Set[Error] = expr match {
      case Let(_, _, _, left, right) => {
        val back = loop(left, relations)
        expr._provenance() = right.provenance
        back ++ loop(right, relations)
      }
      
      case New(_, child) => {
        val back = loop(child, relations)
        expr._provenance() = DynamicProvenance(expr.nodeId)
        back
      }
      
      case Relate(_, from, to, in) => {
        val back = loop(from, relations) ++
          loop(to, relations) ++
          loop(in, relations + ((from.provenance, to.provenance)))
          
        expr._provenance() = in.provenance
        back
      }
      
      case TicVar(_, _) | StrLit(_, _) | NumLit(_, _) | BoolLit(_, _) => {
        expr._provenance() = ValueProvenance
        Set()
      }
      
      case ObjectDef(_, props) => {
        val exprs = props map { case (_, e) => e }
        val errorSets = exprs map { loop(_, relations) }
        val provenances = exprs map { _.provenance }
        val back = errorSets.fold(Set()) { _ ++ _ }
        
        expr._provenance() = provenances.fold(ValueProvenance)(unifyProvenance(relations))
        
        if (expr.provenance == NullProvenance)
          back + Error(expr, Message)
        else
          back
      }
      
      case ArrayDef(_, exprs) => {
        val errorSets = exprs map { loop(_, relations) }
        val provenances = exprs map { _.provenance }
        val back = errorSets.fold(Set()) { _ ++ _ }
        
        expr._provenance() = provenances.fold(ValueProvenance)(unifyProvenance(relations))
        
        if (expr.provenance == NullProvenance)
          back + Error(expr, Message)
        else
          back
      }
      
      case Descent(_, child, _) => {
        val back = loop(child, relations)
        expr._provenance() = child.provenance
        back
      }
      
      case Deref(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        expr._provenance() = unifyProvenance(relations)(left.provenance, right.provenance)
        
        if (expr.provenance == NullProvenance)
          back + Error(expr, Message)
        else
          back
      }
      
      case d @ Dispatch(_, _, exprs) => {
        val errorSets = exprs map { loop(_, relations) }
        val provenances = exprs map { _.provenance }
        val back = errorSets.fold(Set()) { _ ++ _ }
        
        val paramProvenance = provenances.fold(ValueProvenance)(unifyProvenance(relations))
        
        expr._provenance() = d.binding match {
          case BuiltIn(_) => ValueProvenance     // note: assumes all primitive functions are reductions!
          case UserDef(e) => e.provenance
          case NullBinding => NullProvenance
        }
        
        if (paramProvenance == NullProvenance)
          back + Error(expr, Message)
        else
          back
      }
      
      case Operation(_, left, _, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        expr._provenance() = unifyProvenance(relations)(left.provenance, right.provenance)
        
        if (expr.provenance == NullProvenance)
          back + Error(expr, Message)
        else
          back
      }
      
      case Add(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        expr._provenance() = unifyProvenance(relations)(left.provenance, right.provenance)
        
        if (expr.provenance == NullProvenance)
          back + Error(expr, Message)
        else
          back
      }
      
      case Sub(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        expr._provenance() = unifyProvenance(relations)(left.provenance, right.provenance)
        
        if (expr.provenance == NullProvenance)
          back + Error(expr, Message)
        else
          back
      }
      
      case Mul(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        expr._provenance() = unifyProvenance(relations)(left.provenance, right.provenance)
        
        if (expr.provenance == NullProvenance)
          back + Error(expr, Message)
        else
          back
      }
      
      case Div(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        expr._provenance() = unifyProvenance(relations)(left.provenance, right.provenance)
        
        if (expr.provenance == NullProvenance)
          back + Error(expr, Message)
        else
          back
      }
      
      case Lt(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        expr._provenance() = unifyProvenance(relations)(left.provenance, right.provenance)
        
        if (expr.provenance == NullProvenance)
          back + Error(expr, Message)
        else
          back
      }
      
      case LtEq(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        expr._provenance() = unifyProvenance(relations)(left.provenance, right.provenance)
        
        if (expr.provenance == NullProvenance)
          back + Error(expr, Message)
        else
          back
      }
      
      case Gt(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        expr._provenance() = unifyProvenance(relations)(left.provenance, right.provenance)
        
        if (expr.provenance == NullProvenance)
          back + Error(expr, Message)
        else
          back
      }
      
      case GtEq(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        expr._provenance() = unifyProvenance(relations)(left.provenance, right.provenance)
        
        if (expr.provenance == NullProvenance)
          back + Error(expr, Message)
        else
          back
      }
      
      case Eq(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        expr._provenance() = unifyProvenance(relations)(left.provenance, right.provenance)
        
        if (expr.provenance == NullProvenance)
          back + Error(expr, Message)
        else
          back
      }
      
      case NotEq(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        expr._provenance() = unifyProvenance(relations)(left.provenance, right.provenance)
        
        if (expr.provenance == NullProvenance)
          back + Error(expr, Message)
        else
          back
      }
      
      case And(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        expr._provenance() = unifyProvenance(relations)(left.provenance, right.provenance)
        
        if (expr.provenance == NullProvenance)
          back + Error(expr, Message)
        else
          back
      }
      
      case Or(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        expr._provenance() = unifyProvenance(relations)(left.provenance, right.provenance)
        
        if (expr.provenance == NullProvenance)
          back + Error(expr, Message)
        else
          back
      }
      
      case Comp(_, child) => {
        val back = loop(child, relations)
        expr._provenance() = child.provenance
        
        if (expr.provenance == NullProvenance)
          back + Error(expr, Message)
        else
          back
      }
      
      case Neg(_, child) => {
        val back = loop(child, relations)
        expr._provenance() = child.provenance
        
        if (expr.provenance == NullProvenance)
          back + Error(expr, Message)
        else
          back
      }
      
      case Paren(_, child) => {
        val back = loop(child, relations)
        expr._provenance() = child.provenance
        
        if (expr.provenance == NullProvenance)
          back + Error(expr, Message)
        else
          back
      }
    }
    
    loop(expr, Set())
  }
  
  def unifyProvenance(relations: Set[(Provenance, Provenance)])(p1: Provenance, p2: Provenance) = (p1, p2) match {
    case pair if relations contains pair => DynamicProvenance(System.identityHashCode(pair))
    
    case (StaticProvenance(path1), StaticProvenance(path2)) if path1 == path2 => 
      StaticProvenance(path1)
    
    case (DynamicProvenance(id1), DynamicProvenance(id2)) if id1 == id2 =>
      DynamicProvenance(id1)
    
    case (NullProvenance, p) => p
    case (p, NullProvenance) => p
    
    case (ValueProvenance, p) => p
    case (p, ValueProvenance) => p
    
    case _ => NullProvenance
  }
  
  sealed trait Provenance
  
  case class StaticProvenance(path: String) extends Provenance
  case class DynamicProvenance(id: Int) extends Provenance
  case object ValueProvenance extends Provenance
  case object NullProvenance extends Provenance
}
