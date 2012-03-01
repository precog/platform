/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.quirrel
package typer

trait Binder extends parser.AST {
  import ast._

  object BuiltIns {
    val Count   = BuiltIn(Identifier(Vector(), "count"), 1, true)
    val Load    = BuiltIn(Identifier(Vector(), "load"), 1, false)
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
    val TimeDifference = BuiltIn(Identifier(Vector("std", "time"), "timeDifference"), 2, false)

    val MillisToISO = BuiltIn(Identifier(Vector("std", "time"), "millisToISO"), 2, false)
    //val ISOToMillis = BuiltIn(Identifier(Vector("std", "time"), "isoToMillis"), 1, false)

    val TimeZone = BuiltIn(Identifier(Vector("std", "time"), "timeZone"), 1, false)
    val Season = BuiltIn(Identifier(Vector("std", "time"), "season"), 1, false) 

    val Year = BuiltIn(Identifier(Vector("std", "time"), "year"), 1, false)
    val QuarterOfYear = BuiltIn(Identifier(Vector("std", "time"), "quarter"), 1, false)
    val MonthOfYear = BuiltIn(Identifier(Vector("std", "time"), "monthOfYear"), 1, false)
    val WeekOfYear = BuiltIn(Identifier(Vector("std", "time"), "weekOfYear"), 1, false)
    val WeekOfMonth = BuiltIn(Identifier(Vector("std", "time"), "weekOfMonth"), 1, false)
    val DayOfYear = BuiltIn(Identifier(Vector("std", "time"), "dayOfYear"), 1, false)
    val DayOfMonth = BuiltIn(Identifier(Vector("std", "time"), "dayOfMonth"), 1, false)
    val DayOfWeek = BuiltIn(Identifier(Vector("std", "time"), "dayOfWeek"), 1, false)
    val HourOfDay = BuiltIn(Identifier(Vector("std", "time"), "hourOfDay"), 1, false)  
    val MinuteOfHour = BuiltIn(Identifier(Vector("std", "time"), "minuteOfHour"), 1, false)
    val SecondOfMinute = BuiltIn(Identifier(Vector("std", "time"), "secondOfMinute"), 1, false)
    val MillisOfSecond = BuiltIn(Identifier(Vector("std", "time"), "millisOfSecond"), 1, false)

    val Date = BuiltIn(Identifier(Vector("std", "time"), "date"), 1, false)
    val YearMonth = BuiltIn(Identifier(Vector("std", "time"), "yearMonth"), 1, false)
    val MonthDay = BuiltIn(Identifier(Vector("std", "time"), "monthDay"), 1, false)
    val DateHour = BuiltIn(Identifier(Vector("std", "time"), "dateHour"), 1, false)
    val DateHourMinute = BuiltIn(Identifier(Vector("std", "time"), "dateHourMin"), 1, false)
    val DateHourMinuteSecond = BuiltIn(Identifier(Vector("std", "time"), "dateHourMinSec"), 1, false)
    val DateHourMinuteSecondMillis = BuiltIn(Identifier(Vector("std", "time"), "dateHourMinSecMilli"), 1, false)
    val TimeWithZone = BuiltIn(Identifier(Vector("std", "time"), "timeWithZone"), 1, false)
    val TimeWithoutZone = BuiltIn(Identifier(Vector("std", "time"), "timeWithoutZone"), 1, false)
    val HourMinute = BuiltIn(Identifier(Vector("std", "time"), "hourMin"), 1, false)
    val HourMinuteSecond = BuiltIn(Identifier(Vector("std", "time"), "hourMinSec"), 1, false)
  }

  val BuiltInFunctions = {
    import BuiltIns._
    import Time._

    Set(Count, Load, Max, Mean, Median, Min, Mode, StdDev, Sum, ChangeTimeZone, TimeDifference, MillisToISO, TimeZone, Season, Year, QuarterOfYear, MonthOfYear, WeekOfYear, WeekOfMonth, DayOfYear, DayOfMonth, DayOfWeek, HourOfDay, MinuteOfHour, SecondOfMinute, MillisOfSecond, Date, YearMonth, DateHour, DateHourMinute, DateHourMinuteSecond, DateHourMinuteSecondMillis, TimeWithZone, TimeWithoutZone, HourMinuteSecond, HourMinute)
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
