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
package com.precog.daze

import org.specs2.mutable._

import com.precog.yggdrasil._
import com.precog.yggdrasil.memoization._

import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.list._
import Iteratee._

import com.precog.common.VectorCase
import com.precog.util.IdGen

case class Precision(p: Double)
class AlmostEqual(d: Double) {
  def ~=(d2: Double)(implicit p: Precision) = (d - d2).abs <= p.p
}

class StatsLibSpec extends Specification
    with Evaluator
    with TestConfigComponent 
    with StatsLib 
    with InfixLib
    with MemoryDatasetConsumer { self =>
      
  import Function._
  
  import dag._
  import instructions._

  val testUID = "testUID"

  def testEval(graph: DepGraph): Set[SEvent] = withContext { ctx =>
    consumeEval(testUID, graph, ctx) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  implicit def add_~=(d: Double) = new AlmostEqual(d)
  implicit val precision = Precision(0.000000000000001)

  "homogenous sets" should {
    "do stuff" in todo
    
    /* "compute rank" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(Rank),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers6"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }

      result2 must contain(1,3,4,5,8,9).only  
    }

    "compute rank within a filter" in {
      val line = Line(0, "")

      val input = Filter(line, None, None,
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers6"))),
        Join(line, Map2Cross(Eq),
          dag.Operate(line, BuiltInFunction1Op(Rank),
            dag.LoadLocal(line, Root(line, PushString("/hom/numbers6")))),
          Root(line, PushNum("5"))))
        
      val result = testEval(input)

      result must haveSize(3)

      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }

      result2 must contain(11).only
    }

    "compute rank within a join" in {
      val line = Line(0, "")

      val input = Join(line, Map2Cross(Add),
        dag.Operate(line, BuiltInFunction1Op(Rank),
          dag.LoadLocal(line, Root(line, PushString("/hom/numbers6")))),
        Root(line, PushNum("2")))
        
      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }

      result2 must contain(3,5,6,7,10,11).only  
    } */
  }  
  
  /* "heterogenous sets" should {
    "compute rank" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(Rank),
        dag.LoadLocal(line, Root(line, PushString("/het/numbers6"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }

      result2 must contain(1,3,4,5,8,9).only  
    }

    "compute rank within an equals filter" in {
      val line = Line(0, "")

      val input = Filter(line, None, None,
        dag.LoadLocal(line, Root(line, PushString("/het/numbers6"))),
        Join(line, Map2Cross(Eq),
          dag.Operate(line, BuiltInFunction1Op(Rank),
            dag.LoadLocal(line, Root(line, PushString("/het/numbers6")))),
          Root(line, PushNum("9"))))
        
      val result = testEval(input)

      result must haveSize(2)

      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }

      result2 must contain(34).only
    }

    "compute rank within another equals filter" in {
      val line = Line(0, "")

      val input = Filter(line, None, None,
        dag.LoadLocal(line, Root(line, PushString("/het/numbers6"))),
        Join(line, Map2Cross(Eq),
          dag.Operate(line, BuiltInFunction1Op(Rank),
            dag.LoadLocal(line, Root(line, PushString("/het/numbers6")))),
          Root(line, PushNum("1"))))
        
      val result = testEval(input)

      result must haveSize(2)

      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }

      result2 must contain(-10).only

    }

    "compute rank within a less-than filter" in {
      val line = Line(0, "")

      val input = Filter(line, None, None,
        dag.LoadLocal(line, Root(line, PushString("/het/numbers6"))),
        Join(line, Map2Cross(LtEq),
          dag.Operate(line, BuiltInFunction1Op(Rank),
            dag.LoadLocal(line, Root(line, PushString("/het/numbers6")))),
          Root(line, PushNum("5"))))
        
      val result = testEval(input)

      result must haveSize(7)

      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }

      result2 must contain(-10,0,5,11).only
    }

    "compute rank within a join" in {
      val line = Line(0, "")

      val input = Join(line, Map2Cross(Add),
        dag.Operate(line, BuiltInFunction1Op(Rank),
          dag.LoadLocal(line, Root(line, PushString("/het/numbers6")))),
        Root(line, PushNum("2")))
        
      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }

      result2 must contain(3,5,6,7,10,11).only  
    }
  }  
  
  "homogenous sets" should {
    "compute denseRank" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(DenseRank),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers6"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }

      result2 must contain(1,2,3,4,5,6).only  
    }

    "compute denseRank within a filter" in {
      val line = Line(0, "")

      val input = Filter(line, None, None,
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers6"))),
        Join(line, Map2Cross(Eq),
          dag.Operate(line, BuiltInFunction1Op(DenseRank),
            dag.LoadLocal(line, Root(line, PushString("/hom/numbers6")))),
          Root(line, PushNum("4"))))
        
      val result = testEval(input)

      result must haveSize(3)

      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }

      result2 must contain(11)
    }

    "compute denseRank within a join" in {
      val line = Line(0, "")

      val input = Join(line, Map2Cross(Add),
        dag.Operate(line, BuiltInFunction1Op(DenseRank),
          dag.LoadLocal(line, Root(line, PushString("/hom/numbers6")))),
        Root(line, PushNum("2")))
        
      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }

      result2 must contain(3,4,5,6,7,8).only  
    }
  }

  "heterogenous sets" should {
    "compute denseRank" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(DenseRank),
        dag.LoadLocal(line, Root(line, PushString("/het/numbers6"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }

      result2 must contain(1,2,3,4,5,6).only  
    }

    "compute denseRank within an equals filter" in {
      val line = Line(0, "")

      val input = Filter(line, None, None,
        dag.LoadLocal(line, Root(line, PushString("/het/numbers6"))),
        Join(line, Map2Cross(Eq),
          dag.Operate(line, BuiltInFunction1Op(DenseRank),
            dag.LoadLocal(line, Root(line, PushString("/het/numbers6")))),
          Root(line, PushNum("6"))))
        
      val result = testEval(input)

      result must haveSize(2)

      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }

      result2 must contain(34)
    }

    "compute denseRank within a less-than filter" in {
      val line = Line(0, "")

      val input = Filter(line, None, None,
        dag.LoadLocal(line, Root(line, PushString("/het/numbers6"))),
        Join(line, Map2Cross(LtEq),
          dag.Operate(line, BuiltInFunction1Op(DenseRank),
            dag.LoadLocal(line, Root(line, PushString("/het/numbers6")))),
          Root(line, PushNum("5"))))
        
      val result = testEval(input)

      result must haveSize(8)

      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }

      result2 must contain(-10,0,5,11,12).only
    }

    "compute denseRank within a join" in {
      val line = Line(0, "")

      val input = Join(line, Map2Cross(Add),
        dag.Operate(line, BuiltInFunction1Op(DenseRank),
          dag.LoadLocal(line, Root(line, PushString("/het/numbers6")))),
        Root(line, PushNum("2")))
        
      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }

      result2 must contain(3,4,5,6,7,8).only  
    }
  }

  "for homogenous sets, the appropriate stats funciton" should {
    "compute linear correlation" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(LinearCorrelation)),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("hom/heightWeight"))),
          Root(line, PushString("height"))),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("hom/heightWeight"))),
          Root(line, PushString("weight"))))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => (d.toDouble ~= 0.9998746737089123)
      }
      
      result2 must contain(true).only
    }  

    "compute covariance" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(Covariance)),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("hom/heightWeight"))),
          Root(line, PushString("height"))),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("hom/heightWeight"))),
          Root(line, PushString("weight"))))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => (d.toDouble ~= 400.08)
      }
      
      result2 must contain(true).only
    }  

    "compute the correct coefficients in a simple linear regression" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(LinearRegression)),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("hom/heightWeight"))),
          Root(line, PushString("height"))),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("hom/heightWeight"))),
          Root(line, PushString("weight"))))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SArray(Vector(SDecimal(slope), SDecimal(yint)))) => {
          val bool1 = slope.toDouble ~= 0.6862906545903664
          val bool2 = yint.toDouble ~= 67.54013997529848
          Vector(bool1, bool2)
        }
      }
      
      result2 must contain(Vector(true, true))
    }  

    "compute the correct coefficients in a simple log regression" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(LogarithmicRegression)),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("hom/heightWeight"))),
          Root(line, PushString("height"))),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("hom/heightWeight"))),
          Root(line, PushString("weight"))))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SArray(Vector(SDecimal(slope), SDecimal(yint)))) => {
          val bool1 = slope.toDouble ~= 38.867859767424704
          val bool2 = yint.toDouble ~= -46.97865418113425
          Vector(bool1, bool2)
        }
      }
      
      result2 must contain(Vector(true, true))
    }  
  }   
  
  "for homogenous sets, in a cross, the appropriate stats function" should {
    "compute linear correlation" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Cross(BuiltInFunction2Op(LinearCorrelation)),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("hom/heightWeight"))),
          Root(line, PushString("height"))),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("hom/heightWeight"))),
          Root(line, PushString("weight"))))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => (d.toDouble ~= 0.9998746737089123)
      }
      
      result2 must contain(true).only
    }  

    "compute covariance" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Cross(BuiltInFunction2Op(Covariance)),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("hom/heightWeight"))),
          Root(line, PushString("height"))),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("hom/heightWeight"))),
          Root(line, PushString("weight"))))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => (d.toDouble ~= 400.08)
      }
      
      result2 must contain(true).only
    }  

    "compute the correct coefficients in a simple linear regression" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Cross(BuiltInFunction2Op(LinearRegression)),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("hom/heightWeight"))),
          Root(line, PushString("height"))),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("hom/heightWeight"))),
          Root(line, PushString("weight"))))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SArray(Vector(SDecimal(slope), SDecimal(yint)))) => {
          val bool1 = slope.toDouble ~= 0.6862906545903664
          val bool2 = yint.toDouble ~= 67.54013997529848
          Vector(bool1, bool2)
        }
      }
      
      result2 must contain(Vector(true, true))
    }  

    "compute the correct coefficients in a simple log regression" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Cross(BuiltInFunction2Op(LogarithmicRegression)),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("hom/heightWeight"))),
          Root(line, PushString("height"))),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("hom/heightWeight"))),
          Root(line, PushString("weight"))))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SArray(Vector(SDecimal(slope), SDecimal(yint)))) => {
          val bool1 = slope.toDouble ~= 38.867859767424704
          val bool2 = yint.toDouble ~= -46.97865418113425
          Vector(bool1, bool2)
        }
      }
      
      result2 must contain(Vector(true, true))
    }  
  }    
  
  "for the same homogenous set, the appropriate stats function" should {
    "compute linear correlation" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(LinearCorrelation)),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("hom/heightWeight"))),
          Root(line, PushString("height"))),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("hom/heightWeight"))),
          Root(line, PushString("height"))))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => (d.toDouble ~= 1)
      }
      
      result2 must contain(true).only //todo test this answer to a certain level of accuracy
    }  

    "compute covariance" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(Covariance)),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("hom/heightWeight"))),
          Root(line, PushString("height"))),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("hom/heightWeight"))),
          Root(line, PushString("height"))))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => (d.toDouble ~= 582.96)
      }
      
      result2 must contain(true).only
    }  

    "compute the correct coefficients in a simple linear regression" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(LinearRegression)),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("hom/heightWeight"))),
          Root(line, PushString("height"))),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("hom/heightWeight"))),
          Root(line, PushString("height"))))

      val result = testEval(input)
      
      result must haveSize(1)

      val result2 = result collect {
        case (VectorCase(), SArray(Vector(SDecimal(slope), SDecimal(yint)))) => {
          val bool1 = slope.toDouble ~= 1
          val bool2 = yint.toDouble ~= 0
          Vector(bool1, bool2)
        }
      }
      
      result2 must contain(Vector(true, true)).only
    } 

    "compute the correct coefficients in a simple log regression" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(LogarithmicRegression)),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("hom/heightWeight"))),
          Root(line, PushString("height"))),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("hom/heightWeight"))),
          Root(line, PushString("height"))))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SArray(Vector(SDecimal(slope), SDecimal(yint)))) => {
          val bool1 = slope.toDouble ~= 56.59154084773969
          val bool2 = yint.toDouble ~= -166.69026355890486
          Vector(bool1, bool2)
        }
      }
      
      result2 must contain(Vector(true, true))
    }   
  }  
  
  "for heterogenous sets, the appropriate stats function" should {
    "compute linear correlation" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(LinearCorrelation)),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("het/heightWeight"))),
          Root(line, PushString("height"))),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("het/heightWeight"))),
          Root(line, PushString("weight"))))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => (d.toDouble ~= 0.9998746737089123)
      }
      
      result2 must contain(true).only 
    }  

    "compute covariance" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(Covariance)),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("het/heightWeight"))),
          Root(line, PushString("height"))),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("het/heightWeight"))),
          Root(line, PushString("weight"))))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => (d.toDouble ~= 400.08)
      }
      
      result2 must contain(true).only
    }  

    "compute the correct coefficients in a simple linear regression" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(LinearRegression)),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("het/heightWeight"))),
          Root(line, PushString("height"))),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("het/heightWeight"))),
          Root(line, PushString("weight"))))

      val result = testEval(input)
      
      result must haveSize(1)
       
      val result2 = result collect {
        case (VectorCase(), SArray(Vector(SDecimal(slope), SDecimal(yint)))) => {
          val bool1 = slope.toDouble ~= 0.6862906545903664
          val bool2 = yint.toDouble ~= 67.54013997529848
          Vector(bool1, bool2)
        }
      }
      
      result2 must contain(Vector(true, true)).only
    }  

    "compute the correct coefficients in a simple log regression" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(LogarithmicRegression)),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("het/heightWeight"))),
          Root(line, PushString("height"))),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("het/heightWeight"))),
          Root(line, PushString("weight"))))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SArray(Vector(SDecimal(slope), SDecimal(yint)))) => {
          val bool1 = slope.toDouble ~= 38.867859767424704
          val bool2 = yint.toDouble ~= -46.97865418113425
          Vector(bool1, bool2)
        }
      }
      
      result2 must contain(Vector(true, true))
    }  
  } 

  "for a homogenous set and a value, the appropriate stats function" should {
    "compute linear correlation" >> {
      "with value on the right" >> {
        val line = Line(0, "")
        
        val input = Join(line, Map2Cross(BuiltInFunction2Op(LinearCorrelation)),
          Join(line, Map2Cross(DerefObject),
            dag.LoadLocal(line, Root(line, PushString("hom/heightWeight"))),
            Root(line, PushString("height"))),
          Root(line, PushNum("5")))

        val result = testEval(input)
        
        result must haveSize(0)
      }      
    
      "with value on the left" >> {
        val line = Line(0, "")
        
        val input = Join(line, Map2Cross(BuiltInFunction2Op(LinearCorrelation)),
          Root(line, PushNum("5")),
          Join(line, Map2Cross(DerefObject),
            dag.LoadLocal(line, Root(line, PushString("hom/heightWeight"))),
            Root(line, PushString("height"))))

        val result = testEval(input)
        
        result must haveSize(0)
      }
    }  

    "compute covariance" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Cross(BuiltInFunction2Op(Covariance)),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("hom/heightWeight"))),
          Root(line, PushString("height"))),
        Root(line, PushNum("5")))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => (d.toDouble ~= 0)
      }
      
      result2 must contain(true)
    }  

    "compute the correct coefficients in a simple linear regression" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Cross(BuiltInFunction2Op(LinearRegression)),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("hom/heightWeight"))),
          Root(line, PushString("height"))),
        Root(line, PushNum("5")))

      val result = testEval(input)
      
      result must haveSize(1)
       
      val result2 = result collect {
        case (VectorCase(), SArray(Vector(SDecimal(slope), SDecimal(yint)))) => {
          val bool1 = slope.toDouble ~= 0
          val bool2 = yint.toDouble ~= 5
          Vector(bool1, bool2)
        }
      }
      
      result2 must contain(Vector(true, true)).only
    } 

    "compute the correct coefficients in a simple log regression" >> {
      "with a positive constant y-value" >> {
        val line = Line(0, "")
        
        val input = Join(line, Map2Cross(BuiltInFunction2Op(LogarithmicRegression)),
          Join(line, Map2Cross(DerefObject),
            dag.LoadLocal(line, Root(line, PushString("hom/heightWeight"))),
            Root(line, PushString("height"))),
          Root(line, PushNum("5")))

        val result = testEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (VectorCase(), SArray(Vector(SDecimal(slope), SDecimal(yint)))) => {
            val bool1 = slope.toDouble ~= 0
            val bool2 = yint.toDouble ~= 5
            Vector(bool1, bool2)
          }
        }
        
        result2 must contain(Vector(true, true))
      }

      "with a negative constant x-value" >> {
        val line = Line(0, "")
        
        val input = Join(line, Map2Cross(BuiltInFunction2Op(LogarithmicRegression)),
          Operate(line, Neg, 
            Root(line, PushNum("5"))),
          Join(line, Map2Cross(DerefObject),
            dag.LoadLocal(line, Root(line, PushString("hom/heightWeight"))),
            Root(line, PushString("height"))))

        val result = testEval(input)
        
        result must haveSize(0)
      }

      "with a negative x-value in one object" >> {
        val line = Line(0, "")
        
        val input = Join(line, Map2Cross(BuiltInFunction2Op(LogarithmicRegression)),
          Join(line, Map2Cross(DerefObject),
            dag.LoadLocal(line, Root(line, PushString("hom/heightWeight_neg"))),
            Root(line, PushString("height"))),
          Join(line, Map2Cross(DerefObject),
            dag.LoadLocal(line, Root(line, PushString("hom/heightWeight_neg"))),
            Root(line, PushString("weight"))))

        val result = testEval(input)
        
        result must haveSize(1) 
        
        val result2 = result collect {
          case (VectorCase(), SArray(Vector(SDecimal(slope), SDecimal(yint)))) => {
            val bool1 = slope.toDouble ~= 38.867859767424704
            val bool2 = yint.toDouble ~= -46.97865418113425
            Vector(bool1, bool2)
          }
        }
        
        result2 must contain(Vector(true, true))
      }
    }   
  } */
}
