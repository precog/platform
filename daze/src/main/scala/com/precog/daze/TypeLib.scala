package com.precog
package daze

import bytecode._

import yggdrasil._
import yggdrasil.table._
import TransSpecModule._

trait TypeLibModule[M[+_]] extends ColumnarTableLibModule[M] {
  trait TypeLib extends ColumnarTableLib {
    import trans._

    val TypeNamespace = Vector("std", "type")

    override def _lib1 = super._lib1 ++ Set(isNumber, isBoolean, isNull, isString, isObject, isArray)

    object isNumber extends Op1(TypeNamespace, "isNumber") {
      val tpe = UnaryOperationType(JType.JUniverseT, JBooleanT)
      def spec[A <: SourceType](ctx: MorphContext)(source: TransSpec[A]): TransSpec[A] =
        trans.IsType(source, JNumberT)
    }

    object isBoolean extends Op1(TypeNamespace, "isBoolean") {
      val tpe = UnaryOperationType(JType.JUniverseT, JBooleanT)
      def spec[A <: SourceType](ctx: MorphContext)(source: TransSpec[A]): TransSpec[A] =
        trans.IsType(source, JBooleanT)
    }

    object isNull extends Op1(TypeNamespace, "isNull") {
      val tpe = UnaryOperationType(JType.JUniverseT, JBooleanT)
      def spec[A <: SourceType](ctx: MorphContext)(source: TransSpec[A]): TransSpec[A] =
        trans.IsType(source, JNullT)
    }

    object isString extends Op1(TypeNamespace, "isString") {
      val tpe = UnaryOperationType(JType.JUniverseT, JBooleanT)
      def spec[A <: SourceType](ctx: MorphContext)(source: TransSpec[A]): TransSpec[A] =
        trans.IsType(source, JTextT)
    }

    object isObject extends Op1(TypeNamespace, "isObject") {
      val tpe = UnaryOperationType(JType.JUniverseT, JBooleanT)
      def spec[A <: SourceType](ctx: MorphContext)(source: TransSpec[A]): TransSpec[A] =
        trans.IsType(source, JObjectUnfixedT)
    }

    object isArray extends Op1(TypeNamespace, "isArray") {
      val tpe = UnaryOperationType(JType.JUniverseT, JBooleanT)
      def spec[A <: SourceType](ctx: MorphContext)(source: TransSpec[A]): TransSpec[A] =
        trans.IsType(source, JArrayUnfixedT)
    }
  }
}
