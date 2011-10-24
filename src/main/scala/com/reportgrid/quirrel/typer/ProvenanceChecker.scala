package com.reportgrid.quirrel
package typer

trait ProvenanceChecker extends parser.AST with Binder {
  
  override def checkProvenance(expr: Expr) = {
    def loop(expr: Expr, relations: Set[(Provenance, Provenance)]): Set[Error] = expr match {
      case Let(_, _, _, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        expr._provenance() = right.provenance
        back
      }
      
      case New(_, child) => {
        val back = loop(child, relations)
        expr._provenance() = DynamicProvenance(expr.nodeId)
        back
      }
      
      case Relate(_, from, to, in) => {
        val back = loop(from, relations) ++ loop(to, relations)
        
        val recursive = if (from.provenance == NullProvenance || to.provenance == NullProvenance) {
          expr._provenance() = NullProvenance
          Set()
        } else if (from.provenance == to.provenance || relations.contains(from.provenance -> to.provenance)) {
          expr._provenance() = NullProvenance
          Set(Error(expr, AlreadyRelatedSets))
        } else {
          val back = loop(in, relations + ((from.provenance, to.provenance)))
          expr._provenance() = in.provenance
          back
        }
        
        back ++ recursive
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
        
        val result = provenances.foldLeft(Some(ValueProvenance): Option[Provenance]) { (left, right) =>
          left flatMap { unifyProvenance(relations)(_, right) }
        }
        
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case ArrayDef(_, exprs) => {
        val errorSets = exprs map { loop(_, relations) }
        val provenances = exprs map { _.provenance }
        val back = errorSets.fold(Set()) { _ ++ _ }
        
        val result = provenances.foldLeft(Some(ValueProvenance): Option[Provenance]) { (left, right) =>
          left flatMap { unifyProvenance(relations)(_, right) }
        }
        
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
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
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case d @ Dispatch(_, _, exprs) => {
        val errorSets = exprs map { loop(_, relations) }
        val provenances = exprs map { _.provenance }
        val back = errorSets.fold(Set()) { _ ++ _ }
        
        lazy val pathParam = exprs.headOption collect {
          case StrLit(_, value) => value
        }
        
        val (prov, errors) = d.binding match {
          case BuiltIn("dataset") =>
            (pathParam map StaticProvenance getOrElse DynamicProvenance(System.identityHashCode(pathParam)), Set())
          
          case BuiltIn(_) => (ValueProvenance, Set())     // note: assumes all primitive functions are reductions!
          
          case UserDef(e) => e.left.provenance match {
            case ValueProvenance => {
              if (exprs.length == e.params.length) {
                val paramProvenance = provenances.foldLeft(Some(ValueProvenance): Option[Provenance]) { (left, right) =>
                  left flatMap { unifyProvenance(relations)(_, right) }
                }
                
                paramProvenance map { p => (p, Set()) } getOrElse (NullProvenance, Set(Error(expr, OperationOnUnrelatedSets)))
              } else {
                (NullProvenance, Set(Error(expr, IncorrectArity(e.params.length, exprs.length))))
              }
            }
            
            case _: StaticProvenance | _: DynamicProvenance => {
              if (exprs.length <= e.params.length) {
                val paramErrors = exprs flatMap {
                  case e if e.provenance != ValueProvenance =>
                    Set(Error(e, SetFunctionAppliedToSet))
                  
                  case _ => Set[Error]()
                }
                
                if (paramErrors.isEmpty)
                  (e.left.provenance, Set())
                else
                  (NullProvenance, paramErrors)
              } else {
                (NullProvenance, Set(Error(expr, IncorrectArity(e.params.length, exprs.length))))
              }
            }
            
            case NullProvenance => (NullProvenance, Set())
          }
          
          case NullBinding => (NullProvenance, Set())
        }
        
        expr._provenance() = prov
        back ++ errors
      }
      
      case Operation(_, left, _, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case Add(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case Sub(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case Mul(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case Div(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case Lt(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case LtEq(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case Gt(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case GtEq(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case Eq(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case NotEq(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case And(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case Or(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case Comp(_, child) => {
        val back = loop(child, relations)
        expr._provenance() = child.provenance
        back
      }
      
      case Neg(_, child) => {
        val back = loop(child, relations)
        expr._provenance() = child.provenance
        back
      }
      
      case Paren(_, child) => {
        val back = loop(child, relations)
        expr._provenance() = child.provenance
        back
      }                                    
    }                                                                           
                                                                                      
    loop(expr, Set())
  }                                                                 
  
  def unifyProvenance(relations: Set[(Provenance, Provenance)])(p1: Provenance, p2: Provenance) = (p1, p2) match {
    case pair if relations contains pair => 
      Some(DynamicProvenance(System.identityHashCode(pair)))
    
    case (StaticProvenance(path1), StaticProvenance(path2)) if path1 == path2 => 
      Some(StaticProvenance(path1))
    
    case (DynamicProvenance(id1), DynamicProvenance(id2)) if id1 == id2 =>
      Some(DynamicProvenance(id1))
    
    case (NullProvenance, p) => Some(NullProvenance)
    case (p, NullProvenance) => Some(NullProvenance)
    
    case (ValueProvenance, p) => Some(p)
    case (p, ValueProvenance) => Some(p)
    
    case _ => None
  }
  
  sealed trait Provenance
  
  case class StaticProvenance(path: String) extends Provenance {
    override val toString = path
  }
  
  case class DynamicProvenance(id: Int) extends Provenance {
    override val toString = "@" + id
  }
  
  case object ValueProvenance extends Provenance {
    override val toString = "<value>"
  }
  
  case object NullProvenance extends Provenance {
    override val toString = "<null>"
  }
}
