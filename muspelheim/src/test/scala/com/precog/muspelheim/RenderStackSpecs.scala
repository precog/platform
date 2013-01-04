package com.precog
package muspelheim

import akka.dispatch.Future

import com.precog.common._
import com.precog.common.json._
import com.precog.daze.EvaluationContext
import com.precog.yggdrasil.table.ColumnarTableModule

import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.syntax.copointed._

trait RenderStackSpecs extends EvalStackSpecs 
    with ParseEvalStack[Future]
    with ColumnarTableModule[Future]
    with Logging {

  implicit val M: Monad[Future] with Copointed[Future]

  "full stack rendering" should {
    def evalTable(str: String, debug: Boolean = false): Table = {
      import trans._
      
      logger.debug("Beginning evaluation of query: " + str)
      
      val forest = compile(str) filter { _.errors.isEmpty }
      forest must haveSize(1)
      
      val tree = forest.head
      tree.errors must beEmpty
      val Right(dag) = decorate(emit(tree))
      val tableM = eval(dag, EvaluationContext("dummyAPIKey", Path.Root, new org.joda.time.DateTime()), true)
      tableM map { _ transform DerefObjectStatic(Leaf(Source), CPathField("value")) } copoint
    }
    
    "render a set of numbers interleaved by delimiters" in {
      val stream = evalTable("(//tutorial/transactions).quantity") renderJson ','
      val strings = stream map { _.toString }
      val str = strings.foldLeft("") { _ + _ } copoint
      
      str must contain(",")
    }
  }
}
