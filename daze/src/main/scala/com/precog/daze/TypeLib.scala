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
