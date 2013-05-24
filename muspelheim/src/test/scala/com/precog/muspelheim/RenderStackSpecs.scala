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
package muspelheim

import akka.dispatch.Future

import com.precog.common._

import com.precog.daze._
import com.precog.yggdrasil.table.ColumnarTableModule

import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.syntax.comonad._

trait RenderStackSpecs extends EvalStackSpecs with Logging {
  type TestStack <: EvalStackLike with ParseEvalStack[Future] with ColumnarTableModule[Future] with MemoryDatasetConsumer[Future]

  import stack._

  implicit val ntFuture = NaturalTransformation.refl[Future]

  "full stack rendering" should {
    def evalTable(str: String, debug: Boolean = false): Table = {
      import trans._
      
      logger.debug("Beginning evaluation of query: " + str)
      
      val evaluator = Evaluator[Future](M)

      val forest = compile(str) filter { _.errors.isEmpty }
      forest must haveSize(1)
      
      val tree = forest.head
      tree.errors must beEmpty
      val Right(dag) = decorate(emit(tree))
      val tableM = evaluator.eval(dag, EvaluationContext("dummyAPIKey", Path.Root, new org.joda.time.DateTime()), true)
      tableM map { _ transform DerefObjectStatic(Leaf(Source), CPathField("value")) } copoint
    }
    
    "render a set of numbers interleaved by delimiters" in {
      val stream = evalTable("(//tutorial/transactions).quantity").renderJson("", ",", "")
      val strings = stream map { _.toString }
      val str = strings.foldLeft("") { _ + _ } copoint
      
      str must contain(",")
    }
  }
}

