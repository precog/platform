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
  import StdLib.{BoolFrom, DoubleFrom, LongFrom, NumFrom, StrFrom}

  val StringNamespace = Vector("std", "string")

  override def _lib1 = super._lib1 ++ Set(length, trim, toUpperCase,
    toLowerCase, isEmpty, intern)

  override def _lib2 = super._lib2 ++ Set(equalsIgnoreCase, codePointAt,
    startsWith, lastIndexOf, concat, endsWith, codePointBefore, substring,
    matches, compareTo, compareToIgnoreCase, equals, indexOf)

  private def isValidInt(num: BigDecimal): Boolean = {
    try { 
      num.toIntExact; true
    } catch {
      case e: java.lang.ArithmeticException => { false }
    }
  }

  class Op1SS(name: String, f: String => String)
  extends Op1(StringNamespace, name) {
    val tpe = UnaryOperationType(JTextT, JNumberT)
    def f1: F1 = new CF1P({
      case c: StrColumn => new StrFrom.S(c, _ != null, f)
    })
  }

  object trim extends Op1SS("trim", _.trim)

  object toUpperCase extends Op1SS("toUpperCase", _.toUpperCase)

  object toLowerCase extends Op1SS("toLowerCase", _.toLowerCase)

  object intern extends Op1SS("intern", _.intern)

  object isEmpty extends Op1(StringNamespace, "isEmpty") {
    val tpe = UnaryOperationType(JTextT, JBooleanT)
    def f1: F1 = new CF1P({
      case c: StrColumn => new BoolFrom.S(c, _ != null, _.isEmpty)
    })
  }

  def neitherNull(x: String, y: String) = x != null && y != null

  object length extends Op1(StringNamespace, "length") {
    val tpe = UnaryOperationType(JTextT, JNumberT)
    def f1: F1 = new CF1P({
      case c: StrColumn => new LongFrom.S(c, _ != null, _.length)
    })
  }

  class Op2SSB(name: String, f: (String, String) => Boolean)
  extends Op2(StringNamespace, name) {
    val tpe = BinaryOperationType(JTextT, JTextT, JBooleanT)
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: StrColumn) =>
        new BoolFrom.SS(c1, c2, neitherNull, f)
    })
  }

  object equals extends Op2SSB("equals", _ equals _)

  object equalsIgnoreCase
  extends Op2SSB("equalsIgnoreCase", _ equalsIgnoreCase _)

  object startsWith extends Op2SSB("startsWith", _ startsWith _)

  object endsWith extends Op2SSB("endsWith", _ endsWith _)

  object matches extends Op2SSB("matches", _ matches _)

  object concat extends Op2(StringNamespace, "concat") {
    val tpe = BinaryOperationType(JTextT, JTextT, JTextT)
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: StrColumn) =>
          new StrFrom.SS(c1, c2, neitherNull, _ concat _)
    })
  }

  class Op2SLL(name: String,
    defined: (String, Long) => Boolean,
    f: (String, Long) => Long) extends Op2(StringNamespace, name) {
    val tpe = BinaryOperationType(JTextT, JNumberT, JNumberT)
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: DoubleColumn) =>
        new LongFrom.SD(c1, c2,
          (s, n) => (n % 1 == 0) && defined(s, n.toLong),
          (s, n) => f(s, n.toLong))

      case (c1: StrColumn, c2: LongColumn) =>
        new LongFrom.SL(c1, c2, defined, f)

      case (c1: StrColumn, c2: NumColumn) =>
        new LongFrom.SN(c1, c2,
          (s, n) => (n % 1 == 0) && defined(s, n.toLong),
          (s, n) => f(s, n.toLong))
    })
  }

  object codePointAt extends Op2SLL("codePointAt",
    (s, n) => n >= 0 && s.length > n,
    (s, n) => s.codePointAt(n.toInt))

  object codePointBefore extends Op2SLL("codePointBefore",
    (s, n) => n >= 0 && s.length > n,
    (s, n) => s.codePointBefore(n.toInt))

  object substring extends Op2(StringNamespace, "substring") {
    val tpe = BinaryOperationType(JTextT, JNumberT, JTextT)
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: LongColumn) =>
        new StrFrom.SL(c1, c2,
          (s, n) => n >= 0 && s.length > n,
          _ substring _.toInt)

      case (c1: StrColumn, c2: NumColumn) =>
        new StrFrom.SN(c1, c2,
          (s, n) => n >= 0 && (n % 1 == 0) && s.length > n,
          _ substring _.toInt)

      case (c1: StrColumn, c2: DoubleColumn) =>
        new StrFrom.SD(c1, c2,
          (s, n) => n >= 0 && (n % 1 == 0) && s.length > n,
          _ substring _.toInt)
    })
  }

  class Op2SSL(name: String, f: (String, String) => Long)
  extends Op2(StringNamespace, name) {
    val tpe = BinaryOperationType(JTextT, JTextT, JNumberT)
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: StrColumn) =>
        new LongFrom.SS(c1, c2, neitherNull, f)
    })
  }

  object compareTo extends Op2SSL("compareTo", _ compareTo _)

  object compareToIgnoreCase extends Op2SSL("compareToIgnoreCase",
    _ compareToIgnoreCase _)

  object indexOf extends Op2SSL("indexOf", _ indexOf _)

  object lastIndexOf extends Op2SSL("lastIndexOf", _ lastIndexOf _)
}
