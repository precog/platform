package com.querio.quirrel
package typer

trait ProvenanceChecker extends parser.AST with Binder with CriticalConditionFinder {
  import Function._
  import Utils._
  import ast._
  
  override def checkProvenance(expr: Expr) = {
    def loop(expr: Expr, relations: Map[Provenance, Set[Provenance]]): Set[Error] = expr match {
      case expr @ Let(_, _, params, left, right) => {
        val leftErrors = loop(left, relations)
        
        if (!params.isEmpty && left.provenance != NullProvenance) {
          val assumptions = expr.criticalConditions map {
            case (id, exprs) => {
              val provenances = flattenForest(exprs) map { _.provenance }
              val unified = provenances reduce unifyProvenanceAssumingRelated
              (id -> unified)
            }
          }
          
          val unconstrained = params filterNot (assumptions contains)
          val required = unconstrained.lastOption map { params indexOf _ } map (1 +) getOrElse 0
          
          expr.assumptions = assumptions
          expr.unconstrainedParams = Set(unconstrained: _*)
          expr.requiredParams = required
        } else {
          expr.assumptions = Map()
          expr.unconstrainedParams = Set()
          expr.requiredParams = 0
        }
        
        val rightErrors = loop(right, relations)
        expr.provenance = right.provenance
        
        leftErrors ++ rightErrors
      }
      
      case New(_, child) => {
        val back = loop(child, relations)
        expr.provenance = DynamicProvenance(expr.nodeId)
        back
      }
      
      case Relate(_, from, to, in) => {
        val back = loop(from, relations) ++ loop(to, relations)
        
        val recursive = if (from.provenance == NullProvenance || to.provenance == NullProvenance) {
          expr.provenance = NullProvenance
          Set()
        } else if (from.provenance == ValueProvenance || to.provenance == ValueProvenance) {
          expr.provenance = NullProvenance
          Set(Error(expr, AlreadyRelatedSets))
        } else if (pathExists(relations, from.provenance, to.provenance)) {
          expr.provenance = NullProvenance
          Set(Error(expr, AlreadyRelatedSets))
        } else {
          val fromExisting = relations.getOrElse(from.provenance, Set())
          val toExisting = relations.getOrElse(to.provenance, Set())
          
          val relations2 = relations.updated(from.provenance, fromExisting + to.provenance)
          val relations3 = relations2.updated(to.provenance, toExisting + from.provenance)
          
          val back = loop(in, relations3)
          val possibilities = in.provenance.possibilities
          
          if (possibilities.contains(from.provenance) || possibilities.contains(to.provenance))
            expr.provenance = DynamicProvenance(System.identityHashCode(expr))
          else
            expr.provenance = in.provenance
          
          back
        }
        
        back ++ recursive
      }
      
      case TicVar(_, _) | StrLit(_, _) | NumLit(_, _) | BoolLit(_, _) => {
        expr.provenance = ValueProvenance
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
        
        expr.provenance = result getOrElse NullProvenance
        
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
        
        expr.provenance = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case Descent(_, child, _) => {
        val back = loop(child, relations)
        expr.provenance = child.provenance
        back
      }
      
      case Deref(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr.provenance = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case d @ Dispatch(_, _, exprs) => {
        val errorSets = exprs map { loop(_, relations) }
        val back = errorSets.fold(Set()) { _ ++ _ }
        
        lazy val pathParam = exprs.headOption collect {
          case StrLit(_, value) => value
        }
        
        val (prov, errors) = d.binding match {
          case BuiltIn(BuiltIns.Load.name, arity) => {
            if (exprs.length == arity)
              (pathParam map StaticProvenance getOrElse DynamicProvenance(System.identityHashCode(pathParam)), Set())
            else
              (NullProvenance, Set(Error(expr, IncorrectArity(arity, exprs.length))))
          }
          
          case BuiltIn(_, arity) => {
            if (exprs.length == arity)
              (ValueProvenance, Set())     // note: assumes all primitive functions are reductions!
            else
              (NullProvenance, Set(Error(expr, IncorrectArity(arity, exprs.length))))
          }
          
          case UserDef(e) => {
            if (exprs.length > e.params.length) {
              (NullProvenance, Set(Error(expr, IncorrectArity(e.params.length, exprs.length))))
            } else if (exprs.length < e.requiredParams) {
              val required = e.params drop exprs.length filter e.unconstrainedParams
              (NullProvenance, Set(Error(expr, UnspecifiedRequiredParams(required))))
            } else {
              val errors = for ((id, pe) <- e.params zip exprs; assumed <- e.assumptions get id) yield {
                (assumed, pe.provenance) match {
                  case (StaticProvenance(_), ValueProvenance) => None
                  case (DynamicProvenance(_), ValueProvenance) => None
                  case (ValueProvenance, _) => None
                  case (NullProvenance, _) => None
                  case (_, NullProvenance) => None
                  case _ => Some(Error(pe, SetFunctionAppliedToSet))
                }
              }
              
              val errorSet = Set(errors.flatten: _*)
              if (errorSet.isEmpty) {
                val provenances = exprs map { _.provenance }
                
                val optUnified = provenances.foldLeft(Some(ValueProvenance): Option[Provenance]) { (left, right) =>
                  left flatMap { unifyProvenance(relations)(_, right) }
                }
                
                optUnified match {
                  case Some(unified) => {
                    val resultPoss = e.left.provenance.possibilities
                    val resultIsSet = resultPoss exists {
                      case StaticProvenance(_) => true
                      case DynamicProvenance(_) => true
                      case _ => false
                    }
                    
                    val paramPoss = unified.possibilities
                    val paramsAreSet = paramPoss exists {
                      case StaticProvenance(_) => true
                      case DynamicProvenance(_) => true
                      case _ => false
                    }
                    
                    if (resultIsSet && paramsAreSet)
                      (NullProvenance, Set(Error(expr, SetFunctionAppliedToSet)))
                    else if (resultPoss.contains(NullProvenance) || paramPoss.contains(NullProvenance))
                      (NullProvenance, Set())
                    else {
                      val varAssumptions = e.assumptions ++ Map(e.params zip provenances: _*) map {
                        case (id, prov) => ((id, e), prov)
                      }
                      
                      val resultProv = computeResultProvenance(e.left, relations, varAssumptions)
                      
                      resultProv match {
                        case NullProvenance =>
                          (NullProvenance, Set(Error(expr, FunctionArgsInapplicable)))
                        
                        case _ => (resultProv, Set())
                      }
                    }
                  }
                  
                  case None => (NullProvenance, Set(Error(expr, OperationOnUnrelatedSets)))
                }
              } else {
                (NullProvenance, errorSet)
              }
            }
          }
          
          case NullBinding => (NullProvenance, Set())
        }
        
        expr.provenance = prov
        back ++ errors
      }
      
      case Operation(_, left, _, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr.provenance = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case Add(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr.provenance = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case Sub(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr.provenance = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case Mul(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr.provenance = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case Div(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr.provenance = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case Lt(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr.provenance = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case LtEq(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr.provenance = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case Gt(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr.provenance = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case GtEq(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr.provenance = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case Eq(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr.provenance = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case NotEq(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr.provenance = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case And(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr.provenance = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case Or(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr.provenance = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case Comp(_, child) => {
        val back = loop(child, relations)
        expr.provenance = child.provenance
        back
      }
      
      case Neg(_, child) => {
        val back = loop(child, relations)
        expr.provenance = child.provenance
        back
      }
      
      case Paren(_, child) => {
        val back = loop(child, relations)
        expr.provenance = child.provenance
        back
      }                                    
    }                                                                           
                                                                                      
    loop(expr, Map())
  }
  
  private def computeResultProvenance(body: Expr, relations: Map[Provenance, Set[Provenance]], varAssumptions: Map[(String, Let), Provenance]): Provenance = body match {
    case body @ Let(_, _, _, left, right) =>
      computeResultProvenance(right, relations, varAssumptions)
    
    case Relate(_, from, to, in) => {
      val fromExisting = relations.getOrElse(from.provenance, Set())
      val toExisting = relations.getOrElse(to.provenance, Set())
      
      val relations2 = relations.updated(from.provenance, fromExisting + to.provenance)
      val relations3 = relations2.updated(to.provenance, toExisting + from.provenance)
      
      val possibilities = in.provenance.possibilities
      
      if (possibilities.contains(from.provenance) || possibilities.contains(to.provenance))
        DynamicProvenance(System.identityHashCode(body))
      else
        computeResultProvenance(in, relations3, varAssumptions)
    }
    
    case t @ TicVar(_, id) => t.binding match {
      case UserDef(lt) => varAssumptions get (id -> lt) getOrElse t.provenance
      case _ => t.provenance
    }
    
    case ObjectDef(_, props) => {
      val provenances = props map {
        case (_, e) => computeResultProvenance(e, relations, varAssumptions)
      }
      
      provenances.fold(ValueProvenance: Provenance) { (p1, p2) =>
        unifyProvenance(relations)(p1, p2) getOrElse NullProvenance
      }
    }
    
    case ArrayDef(_, values) => {
      val provenances = values map { e => computeResultProvenance(e, relations, varAssumptions) }
      
      provenances.fold(ValueProvenance: Provenance) { (p1, p2) =>
        unifyProvenance(relations)(p1, p2) getOrElse NullProvenance
      }
    }
    
    case Descent(_, child, _) => computeResultProvenance(child, relations, varAssumptions)
    
    case Deref(_, left, right) => {
      val leftProv = computeResultProvenance(left, relations, varAssumptions)
      val rightProv = computeResultProvenance(right, relations, varAssumptions)
      unifyProvenance(relations)(leftProv, rightProv) getOrElse NullProvenance
    }
    
    case d @ Dispatch(_, _, actuals) => {
      val provenances = actuals map { e => computeResultProvenance(e, relations, varAssumptions) }
      
      if (d.isReduction) {
        d.provenance
      } else {
        d.binding match {
          case UserDef(e) => {
            val varAssumptions2 = e.assumptions ++ Map(e.params zip provenances: _*) map {
              case (id, prov) => ((id, e), prov)
            }
            
            computeResultProvenance(e.left, relations, varAssumptions ++ varAssumptions2)
          }
          
          case _ => d.provenance
        }
      }
    }
    
    case Operation(_, left, _, right) => {
      val leftProv = computeResultProvenance(left, relations, varAssumptions)
      val rightProv = computeResultProvenance(right, relations, varAssumptions)
      unifyProvenance(relations)(leftProv, rightProv) getOrElse NullProvenance
    }
    
    case Add(_, left, right) => {
      val leftProv = computeResultProvenance(left, relations, varAssumptions)
      val rightProv = computeResultProvenance(right, relations, varAssumptions)
      unifyProvenance(relations)(leftProv, rightProv) getOrElse NullProvenance
    }
    
    case Sub(_, left, right) => {
      val leftProv = computeResultProvenance(left, relations, varAssumptions)
      val rightProv = computeResultProvenance(right, relations, varAssumptions)
      unifyProvenance(relations)(leftProv, rightProv) getOrElse NullProvenance
    }
    
    case Mul(_, left, right) => {
      val leftProv = computeResultProvenance(left, relations, varAssumptions)
      val rightProv = computeResultProvenance(right, relations, varAssumptions)
      unifyProvenance(relations)(leftProv, rightProv) getOrElse NullProvenance
    }
    
    case Div(_, left, right) => {
      val leftProv = computeResultProvenance(left, relations, varAssumptions)
      val rightProv = computeResultProvenance(right, relations, varAssumptions)
      unifyProvenance(relations)(leftProv, rightProv) getOrElse NullProvenance
    }
    
    case Lt(_, left, right) => {
      val leftProv = computeResultProvenance(left, relations, varAssumptions)
      val rightProv = computeResultProvenance(right, relations, varAssumptions)
      unifyProvenance(relations)(leftProv, rightProv) getOrElse NullProvenance
    }
    
    case LtEq(_, left, right) => {
      val leftProv = computeResultProvenance(left, relations, varAssumptions)
      val rightProv = computeResultProvenance(right, relations, varAssumptions)
      unifyProvenance(relations)(leftProv, rightProv) getOrElse NullProvenance
    }
    
    case Gt(_, left, right) => {
      val leftProv = computeResultProvenance(left, relations, varAssumptions)
      val rightProv = computeResultProvenance(right, relations, varAssumptions)
      unifyProvenance(relations)(leftProv, rightProv) getOrElse NullProvenance
    }
    
    case GtEq(_, left, right) => {
      val leftProv = computeResultProvenance(left, relations, varAssumptions)
      val rightProv = computeResultProvenance(right, relations, varAssumptions)
      unifyProvenance(relations)(leftProv, rightProv) getOrElse NullProvenance
    }
    
    case Eq(_, left, right) => {
      val leftProv = computeResultProvenance(left, relations, varAssumptions)
      val rightProv = computeResultProvenance(right, relations, varAssumptions)
      unifyProvenance(relations)(leftProv, rightProv) getOrElse NullProvenance
    }
    
    case NotEq(_, left, right) => {
      val leftProv = computeResultProvenance(left, relations, varAssumptions)
      val rightProv = computeResultProvenance(right, relations, varAssumptions)
      unifyProvenance(relations)(leftProv, rightProv) getOrElse NullProvenance
    }
    
    case And(_, left, right) => {
      val leftProv = computeResultProvenance(left, relations, varAssumptions)
      val rightProv = computeResultProvenance(right, relations, varAssumptions)
      unifyProvenance(relations)(leftProv, rightProv) getOrElse NullProvenance
    }
    
    case Or(_, left, right) => {
      val leftProv = computeResultProvenance(left, relations, varAssumptions)
      val rightProv = computeResultProvenance(right, relations, varAssumptions)
      unifyProvenance(relations)(leftProv, rightProv) getOrElse NullProvenance
    }
    
    case Comp(_, child) => computeResultProvenance(child, relations, varAssumptions)
    
    case Neg(_, child) => computeResultProvenance(child, relations, varAssumptions)
    
    case Paren(_, child) => computeResultProvenance(child, relations, varAssumptions)
    
    case New(_, _) | StrLit(_, _) | NumLit(_, _) | BoolLit(_, _) => body.provenance
  }
  
  private def unifyProvenance(relations: Map[Provenance, Set[Provenance]])(p1: Provenance, p2: Provenance): Option[Provenance] = (p1, p2) match {
    case (p1, p2) if pathExists(relations, p1, p2) || pathExists(relations, p2, p1) => 
      Some(p1 & p2)
    
    case (UnionProvenance(left, right), p2) => {
      val leftP = unifyProvenance(relations)(left, p2)
      val rightP = unifyProvenance(relations)(right, p2)
      val unionP = (leftP.toList zip rightP.toList headOption) map {
        case (p1, p2) => p1 & p2
      }
      
      unionP orElse leftP orElse rightP
    }
    
    case (p1, UnionProvenance(left, right)) => {
      val leftP = unifyProvenance(relations)(p1, left)
      val rightP = unifyProvenance(relations)(p1, right)
      val unionP = (leftP.toList zip rightP.toList headOption) map {
        case (p1, p2) => p1 & p2
      }
      
      unionP orElse leftP orElse rightP
    }
    
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
  
  private def pathExists(graph: Map[Provenance, Set[Provenance]], from: Provenance, to: Provenance): Boolean = {
    // not actually DFS, but that's alright since we can't have cycles
    def dfs(seen: Set[Provenance])(from: Provenance): Boolean = {
      if (seen contains from) {
        false
      } else if (from == to) {
        true
      } else {
        val next = graph.getOrElse(from, Set())
        val seen2 = seen + from
        next exists dfs(seen2)
      }
    }
    
    dfs(Set())(from)
  }
  
  private def unifyProvenanceAssumingRelated(p1: Provenance, p2: Provenance) = (p1, p2) match {
    case (StaticProvenance(path1), StaticProvenance(path2)) if path1 == path2 => 
      StaticProvenance(path1)
    
    case (DynamicProvenance(id1), DynamicProvenance(id2)) if id1 == id2 =>
      DynamicProvenance(id1)
    
    case (NullProvenance, p) => NullProvenance
    case (p, NullProvenance) => NullProvenance
    
    case (ValueProvenance, p) => p
    case (p, ValueProvenance) => p
    
    case pair => DynamicProvenance(System.identityHashCode(pair))
  }
  
  private def flattenForest(forest: Set[ConditionTree]): Set[Expr] = forest flatMap {
    case Condition(expr) => Set(expr)
    case Reduction(_, forest) => flattenForest(forest)
  }
  
  
  sealed trait Provenance {
    def &(that: Provenance) = (this, that) match {
      case (`that`, `that`) => that
      case (NullProvenance, _) => that
      case (_, NullProvenance) => this
      case _ => UnionProvenance(this, that)
    }
    
    def possibilities = Set(this)
  }
  
  case class UnionProvenance(left: Provenance, right: Provenance) extends Provenance {
    override val toString = "(%s & %s)".format(left, right)
    
    override def possibilities = left.possibilities ++ right.possibilities + this
  }
  
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
