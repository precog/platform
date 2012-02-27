package com.precog.quirrel
package typer

trait Binder extends parser.AST {
  import ast._

  object BuiltIns {
    val Count   = BuiltIn(Identifier(Vector(), "count"), 1, true)
    val Load    = BuiltIn(Identifier(Vector(), "dataset"), 1, false)
    val Max     = BuiltIn(Identifier(Vector(), "max"), 1, true)
    val Mean    = BuiltIn(Identifier(Vector(), "mean"), 1, true)
    val Median  = BuiltIn(Identifier(Vector(), "median"), 1, true)
    val Min     = BuiltIn(Identifier(Vector(), "min"), 1, true)
    val Mode    = BuiltIn(Identifier(Vector(), "mode"), 1, true)
    val StdDev  = BuiltIn(Identifier(Vector(), "stdDev"), 1, true)
    val Sum     = BuiltIn(Identifier(Vector(), "sum"), 1, true)
  }

  object Time {
    val ChangeTimeZone = BuiltIn(Identifier(Vector("std", "time"), "changeTimeZone"), 2, false)  
    val EpochToISO = BuiltIn(Identifier(Vector("std", "time"), "epochToISO"), 2, false)
    //val ISOToEpoch = BulitIn(Identifier(Vector("std", "time"), "iSOToEpoch"), 1, false)
    

    //the following are all projections to sets of integers
    //todo truncate hour, truncate minute, and other truncate functions. Note: retain date information 
    

    val Year = BuiltIn(Identifier(Vector("std", "time"), "year"), 1, false)
    val QuarterOfYear = BuiltIn(Identifier(Vector("std", "time"), "quarter"), 1, false)
    val MonthOfYear = BuiltIn(Identifier(Vector("std", "time"), "monthOfYear"), 1, false)
    val WeekOfYear = BuiltIn(Identifier(Vector("std", "time"), "weekOfYear"), 1, false)
    val DayOfYear = BuiltIn(Identifier(Vector("std", "time"), "dayOfYear"), 1, false)
    val DayOfMonth = BuiltIn(Identifier(Vector("std", "time"), "dayOfMonth"), 1, false)
    val DayOfWeek = BuiltIn(Identifier(Vector("std", "time"), "dayOfWeek"), 1, false)
    val HourOfDay = BuiltIn(Identifier(Vector("std", "time"), "hourOfDay"), 1, false)  
    val MinuteOfHour = BuiltIn(Identifier(Vector("std", "time"), "minuteOfHour"), 1, false)
    val SecondOfMinute = BuiltIn(Identifier(Vector("std", "time"), "secondOfMinute"), 1, false)
    val MillisOfSecond = BuiltIn(Identifier(Vector("std", "time"), "millisOfSecond"), 1, false)
  }

  val BuiltInFunctions = {
    import BuiltIns._
    import Time._

    Set(Count, Load, Max, Mean, Median, Min, Mode, StdDev, Sum, ChangeTimeZone, EpochToISO, Year, QuarterOfYear, MonthOfYear, WeekOfYear, DayOfYear, DayOfMonth, DayOfWeek, HourOfDay, MinuteOfHour, SecondOfMinute, MillisOfSecond)
  }

  override def bindNames(tree: Expr) = {
    def loop(tree: Expr, env: Map[Either[TicId, Identifier], Binding]): Set[Error] = tree match {
      case b @ Let(_, id, formals, left, right) => {
        val env2 = formals.foldLeft(env) { (m, s) => m + (Left(s) -> UserDef(b)) }
        loop(left, env2) ++ loop(right, env + (Right(id) -> UserDef(b)))
      }
      
      case New(_, child) => loop(child, env)
      
      case Relate(_, from, to, in) =>
        loop(from, env) ++ loop(to, env) ++ loop(in, env)
      
      case t @ TicVar(_, name) => {
        env get Left(name) match {
          case Some(b @ UserDef(_)) => {
            t.binding = b
            Set()
          }
          
          case None => {
            t.binding = NullBinding
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
        if (env contains Right(name)) {
          d.binding = env(Right(name))
          
          d.isReduction = env(Right(name)) match {
            case BuiltIn(BuiltIns.Load.name, _, _) => false
            case BuiltIn(_, _, true) => true
            case BuiltIn(_, _, false) => false
            case _ => false
          }
          
          recursive
        } else {
          d.binding = NullBinding
          d.isReduction = false
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
    
    loop(tree, BuiltInFunctions.map({ b => Right(b.name) -> b})(collection.breakOut))
  } 

  sealed trait Binding
  sealed trait FormalBinding extends Binding
  
  // TODO arity and types
  case class BuiltIn(name: Identifier, arity: Int, reduction: Boolean) extends Binding {
    override val toString = "<native: %s(%d)>".format(name, arity)
  }
  
  case class UserDef(b: Let) extends Binding with FormalBinding {
    override val toString = "@%d".format(b.nodeId)
  }
  
  case object NullBinding extends Binding with FormalBinding {
    override val toString = "<null>"
  }
}
