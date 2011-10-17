package com.reportgrid.quirrel
package typer

trait Binder extends parser.AST {
  val BuiltInFunctions = Set(
    BuiltIn("count"),
    BuiltIn("dataset"),
    BuiltIn("max"),
    BuiltIn("mean"),
    BuiltIn("median"),
    BuiltIn("min"),
    BuiltIn("mode"),
    BuiltIn("stdDev"),
    BuiltIn("sum"))
  
  override def bindNames(tree: Expr) = {
    def loop(tree: Expr, env: Map[String, Binding]): Set[Error] = tree match {
      case b @ Let(id, _, left, right) =>
        loop(left, env) ++ loop(right, env + (id -> UserDef(b)))
      
      case New(child) => loop(child, env)
      
      case Relate(from, to, in) =>
        loop(from, env) ++ loop(to, env) ++ loop(in, env)
      
      case TicVar(_) => Set()
        
      case StrLit(_) => Set()
      
      case NumLit(_) => Set()
      
      case BoolLit(_) => Set()
      
      case ObjectDef(props) => {
        val results = for ((_, e) <- props)
          yield loop(e, env)
        
        results.fold(Set()) { _ ++ _ }
      }
      
      case ArrayDef(values) =>
        (values map { loop(_, env) }).fold(Set()) { _ ++ _ }
      
      case Descent(child, _) => loop(child, env)
      
      case Deref(left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case d @ Dispatch(name, actuals) => {
        val recursive = (actuals map { loop(_, env) }).fold(Set()) { _ ++ _ }
        if (env contains name) {
          d._binding() = env(name)
          recursive
        } else {
          d._binding() = NullBinding
          recursive + Error(d, "undefined function: %s".format(name))
        }
      }
      
      case Operation(left, _, right) =>
        loop(left, env) ++ loop(right, env)
      
      case Add(left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case Sub(left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case Mul(left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case Div(left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case Lt(left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case LtEq(left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case Gt(left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case GtEq(left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case Eq(left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case NotEq(left, right) =>
        loop(left, env) ++ loop(right, env)
     
      case Comp(child) => loop(child, env)
      
      case Neg(child) => loop(child, env)
      
      case Paren(child) => loop(child, env)
    }
    
    loop(tree, BuiltInFunctions.map({ b => b.name -> b})(collection.breakOut))
  }
  
  sealed trait Binding
  
  // TODO arity and types
  case class BuiltIn(name: String) extends Binding
  
  case class UserDef(b: Let) extends Binding
  
  case object NullBinding extends Binding
}
