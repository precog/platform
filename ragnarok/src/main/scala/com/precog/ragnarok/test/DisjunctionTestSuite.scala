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
//package com.precog
//package ragnarok
//package test
//
//object DisjunctionTestSuite extends PerfTestSuite {
//  query(
//    """
//      | foo := //foo
//      | bar := //bar
//      | solve 'a = foo.a, 'b = foo.b
//      |   c' := bar.c where bar.a = 'a | bar.b = 'b
//      |   { a: 'a, b: 'b, c: c' }
//      |""".stripMargin)
//
//
////      | foo := //foo
////      | bar := //bar
////      | solve 'a, 'b
////      |   foo' := foo where foo.a = 'a & foo.b = 'b
////      |   c' := bar.c where bar.a = 'a | bar.b = 'b
////      |   foo' ~ c'
////      |   { a: 'a, b: 'b, c: c', fa: foo' }
//
//      //      | foo := //foo
////      | solve 'a = foo.a, 'b = foo.b
////      |   { a: 'a, b: 'b }
//
////      | foo := //foo
////      | solve 'a, 'b
////      |   foo' := foo where foo.a = 'a & foo.b = 'b
////      |   { a: foo'.a, b: foo'.b }
//
////      | foo := //foo
////      | { a: foo.a, b: foo.b }
//
//      
////      | foo := //foo
////      | bar := //bar
////      | solve 'a = foo.a, 'b = foo.b
////      |   c' := bar.c where bar.a = 'a | bar.b = 'b
////      |   { a: 'a, b: 'b, c: c' }
//
////      | foo := //foo
////      | bar := //bar
////      | solve 'a, 'b
////      | foo' := foo where foo.a = 'a & foo.b = 'b
////      | bar' := bar where bar.a = 'a
////      |   ['a, 'b, sum(foo'.a), sum(bar'.a)]
//      
//}
