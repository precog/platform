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
package com.precog
package daze

import bytecode.{ BinaryOperationType, UnaryOperationType, JTextT, JNumberT, JBooleanT }
import bytecode.Library

import java.lang.String

import yggdrasil._
import yggdrasil.table._

trait StringLib[M[+_]] extends GenOpcode[M] {
  val StringNamespace = Vector("std", "string")

  override def _lib1 = super._lib1 ++ Set(length, trim, toUpperCase, toLowerCase, isEmpty, intern)
  override def _lib2 = super._lib2 ++ Set(equalsIgnoreCase, codePointAt, startsWith, lastIndexOf, concat, endsWith, codePointBefore, substring, matches, compareTo, compareToIgnoreCase, equals, indexOf)

  private def isValidInt(num: BigDecimal): Boolean = {
    try { num.toIntExact; true
    } catch {
      case e:java.lang.ArithmeticException => { false }
    }
  }

  object length extends Op1(StringNamespace, "length") {
    val tpe = UnaryOperationType(JTextT, JNumberT)
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with LongColumn {
        def apply(row: Int) = c(row).length
      }
    })
    
    /* val operandType = Some(SString)
    val operation: PartialFunction[SValue, SValue] = {
      case SString(str) => SDecimal(str.length)
    } */
  }
  object trim extends Op1(StringNamespace, "trim") {
    val tpe = UnaryOperationType(JTextT, JTextT)
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with StrColumn {
        def apply(row: Int) = c(row).trim
      }
    })
    
    /* val operandType = Some(SString)
    val operation: PartialFunction[SValue, SValue] = {
      case SString(str) => SString(str.trim)
    } */
  }
  object toUpperCase extends Op1(StringNamespace, "toUpperCase") {
    val tpe = UnaryOperationType(JTextT, JTextT)
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with StrColumn {
        def apply(row: Int) = c(row).toUpperCase
      }
    })
    
    /* val operandType = Some(SString)
    val operation: PartialFunction[SValue, SValue] = {
      case SString(str) => SString(str.toUpperCase)
    } */
  }  
  object toLowerCase extends Op1(StringNamespace, "toLowerCase") {
    val tpe = UnaryOperationType(JTextT, JTextT)
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with StrColumn {
        def apply(row: Int) = c(row).toLowerCase
      }
    })
    
    /* val operandType = Some(SString)
    val operation: PartialFunction[SValue, SValue] = {
      case SString(str) => SString(str.toLowerCase)
    } */
  }
  object isEmpty extends Op1(StringNamespace, "isEmpty") {
    val tpe = UnaryOperationType(JTextT, JBooleanT)
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with BoolColumn {
        def apply(row: Int) = c(row).isEmpty
      }
    })
    
    /* val operandType = Some(SString)
    val operation: PartialFunction[SValue, SValue] = {
      case SString(str) => SBoolean(str.isEmpty)
    } */
  }
  object intern extends Op1(StringNamespace, "intern") {
    val tpe = UnaryOperationType(JTextT, JTextT)
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with StrColumn {
        def apply(row: Int) = c(row).intern
      }
    })
    
    /* val operandType = Some(SString)
    val operation: PartialFunction[SValue, SValue] = {
      case SString(str) => SString(str.intern)
    } */
  }
  //object hashCode extends BIF1(StringNamespace, "hashCode") {   //mysterious case - java.lang.ClassCastException
  //  val operandType = Some(SString)
  //  val operation: PartialFunction[SValue, SValue] = {
  //    case SString(str) => SDecimal(str.hashCode)
  //  }
  //}
  object equalsIgnoreCase extends Op2(StringNamespace, "equalsIgnoreCase") {
    val tpe = BinaryOperationType(JTextT, JTextT, JBooleanT)
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: StrColumn) => new Map2Column(c1, c2) with BoolColumn {
        def apply(row: Int) = c1(row).equalsIgnoreCase(c2(row))
      }
    })
    
    /* val operandType = (Some(SString), Some(SString))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SString(v2)) => SBoolean(str.equalsIgnoreCase(v2))
    } */
  }
  object codePointAt extends Op2(StringNamespace, "codePointAt") {
    val tpe = BinaryOperationType(JTextT, JNumberT, JNumberT)
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: LongColumn)  => new Map2Column(c1, c2) with LongColumn {  //todo do we need other cases for other numeric input types?
        def apply(row: Int) = {
          val str = c1(row)
          val num = c2(row)

          if ((num >= 0) && (str.length >= num + 1) && isValidInt(num))
            str.codePointAt(num.toInt)
          else
            sys.error("todo: null?")
        }
      }
    })
    
    /* val operandType = (Some(SString), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SDecimal(v2)) if ((v2 >= 0) && (str.length >= v2 + 1) && isValidInt(v2)) => 
        SDecimal(str.codePointAt(v2.toInt))
    } */
  }
  object startsWith extends Op2(StringNamespace, "startsWith") {
    val tpe = BinaryOperationType(JTextT, JTextT, JBooleanT)
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: StrColumn) => new Map2Column(c1, c2) with BoolColumn {
        def apply(row: Int) = c1(row).startsWith(c2(row))
      }
    })
    
    /* val operandType = (Some(SString), Some(SString))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SString(v2)) => SBoolean(str.startsWith(v2))
    } */
  }
  object lastIndexOf extends Op2(StringNamespace, "lastIndexOf") {
    val tpe = BinaryOperationType(JTextT, JTextT, JNumberT)
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: StrColumn) => new Map2Column(c1, c2) with LongColumn {
        def apply(row: Int) = c1(row).lastIndexOf(c2(row))
      }
    })
    
    /* val operandType = (Some(SString), Some(SString))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SString(v2)) => SDecimal(str.lastIndexOf(v2))
    } */
  }
  object concat extends Op2(StringNamespace, "concat") {
    val tpe = BinaryOperationType(JTextT, JTextT, JTextT)
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: StrColumn) => new Map2Column(c1, c2) with StrColumn {
        def apply(row: Int) = c1(row).concat(c2(row))
      }
    })
    
    /* val operandType = (Some(SString), Some(SString))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SString(v2)) => SString(str.concat(v2))
    } */
  }
  object endsWith extends Op2(StringNamespace, "endsWith") {
    val tpe = BinaryOperationType(JTextT, JTextT, JBooleanT)
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: StrColumn) => new Map2Column(c1, c2) with BoolColumn {
        def apply(row: Int) = c1(row).endsWith(c2(row))
      }
    })
    
    /* val operandType = (Some(SString), Some(SString))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SString(v2)) => SBoolean(str.endsWith(v2))
    } */
  }
  object codePointBefore extends Op2(StringNamespace, "codePointBefore") {
    val tpe = BinaryOperationType(JTextT, JNumberT, JNumberT)
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: LongColumn) => new Map2Column(c1, c2) with LongColumn {
        def apply(row: Int) = {
          val str = c1(row)
          val num = c2(row)

          if ((num >= 0) && (str.length >= num + 1) && isValidInt(num))
            str.codePointBefore(num.toInt)
          else 
            sys.error("todo")
        }
      }
    })
    
    /* val operandType = (Some(SString), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SDecimal(v2)) if ((v2 > 0) && (str.length >= v2) && isValidInt(v2)) => 
        SDecimal(str.codePointBefore(v2.toInt))
    } */
  }
  object substring extends Op2(StringNamespace, "substring") {
    val tpe = BinaryOperationType(JTextT, JNumberT, JTextT)
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: LongColumn) => new Map2Column(c1, c2) with StrColumn {
        def apply(row: Int) = {
          val str = c1(row)
          val num = c2(row)

          if ((num >= 0) && (str.length >= num + 1) && isValidInt(num))
            str.substring(num.toInt)
          else
            sys.error("todo")
        }
      }
    })
    
    /* val operandType = (Some(SString), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SDecimal(v2)) if ((v2 >= 0) && (str.length >= v2 + 1) && isValidInt(v2)) => 
        SString(str.substring(v2.toInt))
    } */
  }
  object matches extends Op2(StringNamespace, "matches") {
    val tpe = BinaryOperationType(JTextT, JTextT, JBooleanT)
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: StrColumn) => new Map2Column(c1, c2) with BoolColumn {
        def apply(row: Int) = c1(row).matches(c2(row))
      }
    })
    
    /* val operandType = (Some(SString), Some(SString))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SString(v2)) => SBoolean(str.matches(v2))
    } */
  }
  object compareTo extends Op2(StringNamespace, "compareTo") {
    val tpe = BinaryOperationType(JTextT, JTextT, JNumberT)
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: StrColumn) => new Map2Column(c1, c2) with LongColumn {
        def apply(row: Int) = c1(row).compareTo(c2(row))
      }
    })
    
    /* val operandType = (Some(SString), Some(SString))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SString(v2)) => SDecimal(str.compareTo(v2))
    } */
  }
  object compareToIgnoreCase extends Op2(StringNamespace, "compareToIgnoreCase") {
    val tpe = BinaryOperationType(JTextT, JTextT, JNumberT)
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: StrColumn) => new Map2Column(c1, c2) with LongColumn {
        def apply(row: Int) = c1(row).compareToIgnoreCase(c2(row))
      }
    })
    
    /* val operandType = (Some(SString), Some(SString))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SString(v2)) => SDecimal(str.compareToIgnoreCase(v2))
    } */
  }
  object equals extends Op2(StringNamespace, "equals") {
    val tpe = BinaryOperationType(JTextT, JTextT, JBooleanT)
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: StrColumn) => new Map2Column(c1, c2) with BoolColumn {
        def apply(row: Int) = c1(row).equals(c2(row))
      }
    })
    
    /* val operandType = (Some(SString), Some(SString))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SString(v2)) => SBoolean(str.equals(v2))
    } */
  }
  object indexOf extends Op2(StringNamespace, "indexOf") {
    val tpe = BinaryOperationType(JTextT, JTextT, JNumberT)
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: StrColumn) => new Map2Column(c1, c2) with LongColumn {
        def apply(row: Int) = c1(row).indexOf(c2(row))
      }
    })
    
    /* val operandType = (Some(SString), Some(SString))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SString(v2)) => SDecimal(str.indexOf(v2))
    } */
  }
}
