package com.reportgrid.quirrel
package typer

trait Binder extends parser.AST {
  val BuiltInFunctions = Set(
    BuiltIn("count", 1),
    BuiltIn("dataset", 1),
    BuiltIn("max", 1),
    BuiltIn("mean", 1),
    BuiltIn("median", 1),
    BuiltIn("min", 1),
    BuiltIn("mode", 1),
    BuiltIn("stdDev", 1),
    BuiltIn("sum", 1))
  
  override def bindNames(tree: Expr) = {
    def loop(tree: Expr, env: Map[String, Binding]): Set[Error] = tree match {
      case b @ Let(_, id, formals, left, right) => {
        val env2 = formals.foldLeft(env) { (m, s) => m + (s -> UserDef(b)) }
        loop(left, env2) ++ loop(right, env + (id -> UserDef(b)))
      }
      
      case New(_, child) => loop(child, env)
      
      case Relate(_, from, to, in) =>
        loop(from, env) ++ loop(to, env) ++ loop(in, env)
      
      case t @ TicVar(_, name) => {
        env get name match {
          case Some(b @ UserDef(_)) => {
            t._binding() = b
            Set()
          }
          
          case None => {
            t._binding() = NullBinding
            Set(Error(t, UndefinedTicVariable(name)))
          }
          
          case _ => throw new AssertionError("Cannot reach this point")
        }
      }
        
      case StrLit(_, _) => Set()
      
      case NumLit(_, _) => Set()
      
      case BoolLit(_, _) => Set()
      
      case ObjectDef(_, props) => {
        val results = for ((_, e) <- props)
          yield loop(e, env)
        
        results.fold(Set()) { _ ++ _ }
      }
      
      case ArrayDef(_, values) =>
        (values map { loop(_, env) }).fold(Set()) { _ ++ _ }
      
      case Descent(_, child, _) => loop(child, env)
      
      case Deref(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case d @ Dispatch(_, name, actuals) => {
        val recursive = (actuals map { loop(_, env) }).fold(Set()) { _ ++ _ }
        if (env contains name) {
          d._binding() = env(name)
          recursive
        } else {
          d._binding() = NullBinding
          recursive + Error(d, UndefinedFunction(name))
        }
      }
      
      case Operation(_, left, _, right) =>
        loop(left, env) ++ loop(right, env)
      
      case Add(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case Sub(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case Mul(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case Div(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case Lt(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case LtEq(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case Gt(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case GtEq(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case Eq(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case NotEq(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case And(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case Or(_, left, right) =>
        loop(left, env) ++ loop(right, env)
     
      case Comp(_, child) => loop(child, env)
      
      case Neg(_, child) => loop(child, env)
      
      case Paren(_, child) => loop(child, env)
    }
    
    loop(tree, BuiltInFunctions.map({ b => b.name -> b})(collection.breakOut))
  }
  
  sealed trait Binding
  sealed trait FormalBinding extends Binding
  
  // TODO arity and types
  case class BuiltIn(name: String, arity: Int) extends Binding {
    override val toString = "<native: %s>".format(name)
  }
  
  case class UserDef(b: Let) extends Binding with FormalBinding {
    override val toString = "@%d".format(b.nodeId)
  }
  
  case object NullBinding extends Binding with FormalBinding {
    override val toString = "<null>"
  }
}
