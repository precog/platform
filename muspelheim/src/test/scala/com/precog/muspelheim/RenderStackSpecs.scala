package com.precog
package muspelheim

import akka.dispatch.Future

import com.precog.common._
import com.precog.common.accounts._

import com.precog.daze._
import com.precog.yggdrasil.execution.EvaluationContext
import com.precog.yggdrasil.table.ColumnarTableModule

import com.weiglewilczek.slf4s.Logging

import org.joda.time.DateTime

import scalaz._
import scalaz.syntax.comonad._

trait RenderStackSpecs extends EvalStackSpecs with Logging {
  type TestStack <: EvalStackLike with ParseEvalStack[Future] with ColumnarTableModule[Future] with MemoryDatasetConsumer[Future]

  import stack._

  implicit val ntFuture = NaturalTransformation.refl[Future]

  private val dummyAccount = AccountDetails("dummyAccount", "nobody@precog.com",
    new DateTime, "dummyAPIKey", Path.Root, AccountPlan.Free)
  private def dummyEvaluationContext = EvaluationContext("dummyAPIKey", dummyAccount, Path.Root, Path.Root, new DateTime)

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
      val tableM = evaluator.eval(dag, dummyEvaluationContext, true)
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

