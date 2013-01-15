package com.precog
package daze

import bytecode._

import yggdrasil._
import yggdrasil.table._
import TransSpecModule._

trait TypeLib[M[+_]] extends GenOpcode[M] {
  import trans._

  val TypeNamespace = Vector("std", "type")

  override def _lib1 = super._lib1 ++ Set(isNumber, isBoolean, isNull, isString, isObject, isArray)

  object isNumber extends Op1(TypeNamespace, "isNumber") {
    val tpe = UnaryOperationType(JType.JUniverseT, JBooleanT)
    def spec[A <: SourceType](ctx: EvaluationContext): TransSpec[A] => TransSpec[A] =
      transSpec => trans.IsType(transSpec, JNumberT)
  }

  object isBoolean extends Op1(TypeNamespace, "isBoolean") {
    val tpe = UnaryOperationType(JType.JUniverseT, JBooleanT)
    def spec[A <: SourceType](ctx: EvaluationContext): TransSpec[A] => TransSpec[A] =
      transSpec => trans.IsType(transSpec, JBooleanT)
  }

  object isNull extends Op1(TypeNamespace, "isNull") {
    val tpe = UnaryOperationType(JType.JUniverseT, JBooleanT)
    def spec[A <: SourceType](ctx: EvaluationContext): TransSpec[A] => TransSpec[A] =
      transSpec => trans.IsType(transSpec, JNullT)
  }

  object isString extends Op1(TypeNamespace, "isString") {
    val tpe = UnaryOperationType(JType.JUniverseT, JBooleanT)
    def spec[A <: SourceType](ctx: EvaluationContext): TransSpec[A] => TransSpec[A] =
      transSpec => trans.IsType(transSpec, JTextT)
  }

  object isObject extends Op1(TypeNamespace, "isObject") {
    val tpe = UnaryOperationType(JType.JUniverseT, JBooleanT)
    def spec[A <: SourceType](ctx: EvaluationContext): TransSpec[A] => TransSpec[A] =
      transSpec => trans.IsType(transSpec, JObjectUnfixedT)
  }

  object isArray extends Op1(TypeNamespace, "isArray") {
    val tpe = UnaryOperationType(JType.JUniverseT, JBooleanT)
    def spec[A <: SourceType](ctx: EvaluationContext): TransSpec[A] => TransSpec[A] =
      transSpec => trans.IsType(transSpec, JArrayUnfixedT)
  }
}
