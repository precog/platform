package com.precog
package daze

import bytecode.Library

import java.lang.String

import yggdrasil._
import yggdrasil.table._

object StringLib extends StringLib

trait StringLib extends GenOpcode with ImplLibrary {
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
    def f1: F1 = new CF1P({
      case c: StrColumn => new StrColumn {
        def isDefinedAt(row: Int) = c.isDefinedAt(row)
        def apply(row: Int) = c(row)
      }
    })
    
    /* val operandType = Some(SString)
    val operation: PartialFunction[SValue, SValue] = {
      case SString(str) => SDecimal(str.length)
    } */
  }
  object trim extends Op1(StringNamespace, "trim") {
    def f1: F1 = new CF1P({
      case c: StrColumn => new StrColumn {
        def isDefinedAt(row: Int) = c.isDefinedAt(row)
        def apply(row: Int) = c(row)
      }
    })
    
    /* val operandType = Some(SString)
    val operation: PartialFunction[SValue, SValue] = {
      case SString(str) => SString(str.trim)
    } */
  }
  object toUpperCase extends Op1(StringNamespace, "toUpperCase") {
    def f1: F1 = new CF1P({
      case c: StrColumn => new StrColumn {
        def isDefinedAt(row: Int) = c.isDefinedAt(row)
        def apply(row: Int) = c(row)
      }
    })
    
    /* val operandType = Some(SString)
    val operation: PartialFunction[SValue, SValue] = {
      case SString(str) => SString(str.toUpperCase)
    } */
  }  
  object toLowerCase extends Op1(StringNamespace, "toLowerCase") {
    def f1: F1 = new CF1P({
      case c: StrColumn => new StrColumn {
        def isDefinedAt(row: Int) = c.isDefinedAt(row)
        def apply(row: Int) = c(row)
      }
    })
    
    /* val operandType = Some(SString)
    val operation: PartialFunction[SValue, SValue] = {
      case SString(str) => SString(str.toLowerCase)
    } */
  }
  object isEmpty extends Op1(StringNamespace, "isEmpty") {
    def f1: F1 = new CF1P({
      case c: StrColumn => new StrColumn {
        def isDefinedAt(row: Int) = c.isDefinedAt(row)
        def apply(row: Int) = c(row)
      }
    })
    
    /* val operandType = Some(SString)
    val operation: PartialFunction[SValue, SValue] = {
      case SString(str) => SBoolean(str.isEmpty)
    } */
  }
  object intern extends Op1(StringNamespace, "intern") {
    def f1: F1 = new CF1P({
      case c: StrColumn => new StrColumn {
        def isDefinedAt(row: Int) = c.isDefinedAt(row)
        def apply(row: Int) = c(row)
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
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: StrColumn) => new StrColumn {
        def isDefinedAt(row: Int) = c1.isDefinedAt(row)
        def apply(row: Int) = c1(row)
      }
    })
    
    /* val operandType = (Some(SString), Some(SString))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SString(v2)) => SBoolean(str.equalsIgnoreCase(v2))
    } */
  }
  object codePointAt extends Op2(StringNamespace, "codePointAt") {
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: StrColumn) => new StrColumn {
        def isDefinedAt(row: Int) = c1.isDefinedAt(row)
        def apply(row: Int) = c1(row)
      }
    })
    
    /* val operandType = (Some(SString), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SDecimal(v2)) if ((v2 >= 0) && (str.length >= v2 + 1) && isValidInt(v2)) => 
        SDecimal(str.codePointAt(v2.toInt))
    } */
  }
  object startsWith extends Op2(StringNamespace, "startsWith") {
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: StrColumn) => new StrColumn {
        def isDefinedAt(row: Int) = c1.isDefinedAt(row)
        def apply(row: Int) = c1(row)
      }
    })
    
    /* val operandType = (Some(SString), Some(SString))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SString(v2)) => SBoolean(str.startsWith(v2))
    } */
  }
  object lastIndexOf extends Op2(StringNamespace, "lastIndexOf") {
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: StrColumn) => new StrColumn {
        def isDefinedAt(row: Int) = c1.isDefinedAt(row)
        def apply(row: Int) = c1(row)
      }
    })
    
    /* val operandType = (Some(SString), Some(SString))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SString(v2)) => SDecimal(str.lastIndexOf(v2))
    } */
  }
  object concat extends Op2(StringNamespace, "concat") {
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: StrColumn) => new StrColumn {
        def isDefinedAt(row: Int) = c1.isDefinedAt(row)
        def apply(row: Int) = c1(row)
      }
    })
    
    /* val operandType = (Some(SString), Some(SString))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SString(v2)) => SString(str.concat(v2))
    } */
  }
  object endsWith extends Op2(StringNamespace, "endsWith") {
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: StrColumn) => new StrColumn {
        def isDefinedAt(row: Int) = c1.isDefinedAt(row)
        def apply(row: Int) = c1(row)
      }
    })
    
    /* val operandType = (Some(SString), Some(SString))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SString(v2)) => SBoolean(str.endsWith(v2))
    } */
  }
  object codePointBefore extends Op2(StringNamespace, "codePointBefore") {
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: StrColumn) => new StrColumn {
        def isDefinedAt(row: Int) = c1.isDefinedAt(row)
        def apply(row: Int) = c1(row)
      }
    })
    
    /* val operandType = (Some(SString), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SDecimal(v2)) if ((v2 > 0) && (str.length >= v2) && isValidInt(v2)) => 
        SDecimal(str.codePointBefore(v2.toInt))
    } */
  }
  object substring extends Op2(StringNamespace, "substring") {
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: StrColumn) => new StrColumn {
        def isDefinedAt(row: Int) = c1.isDefinedAt(row)
        def apply(row: Int) = c1(row)
      }
    })
    
    /* val operandType = (Some(SString), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SDecimal(v2)) if ((v2 >= 0) && (str.length >= v2 + 1) && isValidInt(v2)) => 
        SString(str.substring(v2.toInt))
    } */
  }
  object matches extends Op2(StringNamespace, "matches") {
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: StrColumn) => new StrColumn {
        def isDefinedAt(row: Int) = c1.isDefinedAt(row)
        def apply(row: Int) = c1(row)
      }
    })
    
    /* val operandType = (Some(SString), Some(SString))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SString(v2)) => SBoolean(str.matches(v2))
    } */
  }
  object compareTo extends Op2(StringNamespace, "compareTo") {
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: StrColumn) => new StrColumn {
        def isDefinedAt(row: Int) = c1.isDefinedAt(row)
        def apply(row: Int) = c1(row)
      }
    })
    
    /* val operandType = (Some(SString), Some(SString))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SString(v2)) => SDecimal(str.compareTo(v2))
    } */
  }
  object compareToIgnoreCase extends Op2(StringNamespace, "compareToIgnoreCase") {
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: StrColumn) => new StrColumn {
        def isDefinedAt(row: Int) = c1.isDefinedAt(row)
        def apply(row: Int) = c1(row)
      }
    })
    
    /* val operandType = (Some(SString), Some(SString))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SString(v2)) => SDecimal(str.compareToIgnoreCase(v2))
    } */
  }
  object equals extends Op2(StringNamespace, "equals") {
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: StrColumn) => new StrColumn {
        def isDefinedAt(row: Int) = c1.isDefinedAt(row)
        def apply(row: Int) = c1(row)
      }
    })
    
    /* val operandType = (Some(SString), Some(SString))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SString(v2)) => SBoolean(str.equals(v2))
    } */
  }
  object indexOf extends Op2(StringNamespace, "indexOf") {
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: StrColumn) => new StrColumn {
        def isDefinedAt(row: Int) = c1.isDefinedAt(row)
        def apply(row: Int) = c1(row)
      }
    })
    
    /* val operandType = (Some(SString), Some(SString))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SString(v2)) => SDecimal(str.indexOf(v2))
    } */
  }
}
