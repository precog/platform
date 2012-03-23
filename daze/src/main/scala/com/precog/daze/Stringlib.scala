package com.precog

package daze

import bytecode.Library

import bytecode.BuiltInFunc1

import java.lang.String

import bytecode.BuiltInFunc2

import yggdrasil._

trait Stringlib extends GenOpcode with ImplLibrary {
  override def _lib1 = super._lib1 ++ Set(toString, length, trim, toUpperCase, isEmpty, intern, hashCode)
  override def _lib2 = super._lib2 ++ Set(equalsIgnoreCase, codePointAt, startsWith, lastIndexOf, concat, endsWith, codePointBefore, substring, matches)

  object toString extends BIF1(Vector("std", "string"), "toString") {
    val operandType = Some(SString)
    val operation: PartialFunction[SValue, SValue] = {
      case SString(str) => SString(str.toString)
    }
  }
  object length extends BIF1(Vector("std", "string"), "length") {
    val operandType = Some(SString)
    val operation: PartialFunction[SValue, SValue] = {
      case SString(str) => SDecimal(str.length)
    }
  }
  object trim extends BIF1(Vector("std", "string"), "trim") {
    val operandType = Some(SString)
    val operation: PartialFunction[SValue, SValue] = {
      case SString(str) => SString(str.trim)
    }
  }
  object toUpperCase extends BIF1(Vector("std", "string"), "toUpperCase") {
    val operandType = Some(SString)
    val operation: PartialFunction[SValue, SValue] = {
      case SString(str) => SString(str.toUpperCase)
    }
  }
  object isEmpty extends BIF1(Vector("std", "string"), "isEmpty") {
    val operandType = Some(SString)
    val operation: PartialFunction[SValue, SValue] = {
      case SString(str) => SBoolean(str.isEmpty)
    }
  }
  object intern extends BIF1(Vector("std", "string"), "intern") {
    val operandType = Some(SString)
    val operation: PartialFunction[SValue, SValue] = {
      case SString(str) => SString(str.intern)
    }
  }
  object hashCode extends BIF1(Vector("std", "string"), "hashCode") {
    val operandType = Some(SString)
    val operation: PartialFunction[SValue, SValue] = {
      case SString(str) => SDecimal(str.hashCode)
    }
  }
  object equalsIgnoreCase extends BIF2(Vector("std", "string"), "equalsIgnoreCase") {
    val operandType = (Some(SString), Some(SString))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SString(v2)) => SBoolean(str.equalsIgnoreCase(v2))
    }
  }
  object codePointAt extends BIF2(Vector("std", "string"), "codePointAt") {
    val operandType = (Some(SString), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SDecimal(v2)) => SDecimal(str.codePointAt(v2.toInt))
    }
  }
  object startsWith extends BIF2(Vector("std", "string"), "startsWith") {
    val operandType = (Some(SString), Some(SString))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SString(v2)) => SBoolean(str.startsWith(v2))
    }
  }
  object lastIndexOf extends BIF2(Vector("std", "string"), "lastIndexOf") {
    val operandType = (Some(SString), Some(SString))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SString(v2)) => SDecimal(str.lastIndexOf(v2))
    }
  }
  object concat extends BIF2(Vector("std", "string"), "concat") {
    val operandType = (Some(SString), Some(SString))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SString(v2)) => SString(str.concat(v2))
    }
  }
  object endsWith extends BIF2(Vector("std", "string"), "endsWith") {
    val operandType = (Some(SString), Some(SString))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SString(v2)) => SBoolean(str.endsWith(v2))
    }
  }
  object codePointBefore extends BIF2(Vector("std", "string"), "codePointBefore") {
    val operandType = (Some(SString), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SDecimal(v2)) => SDecimal(str.codePointBefore(v2.toInt))
    }
  }
  object substring extends BIF2(Vector("std", "string"), "substring") {
    val operandType = (Some(SString), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SDecimal(v2)) => SString(str.substring(v2.toInt))
    }
  }
  object matches extends BIF2(Vector("std", "string"), "matches") {
    val operandType = (Some(SString), Some(SString))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(str), SString(v2)) => SBoolean(str.matches(v2))
    }
  }
}
