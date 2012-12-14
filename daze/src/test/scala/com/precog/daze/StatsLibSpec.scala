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
import com.precog.common.Path

import scalaz._
import scalaz.std.list._

import com.precog.util.IdGen

case class Precision(p: Double)
class AlmostEqual(d: Double) {
  def ~=(d2: Double)(implicit p: Precision) = (d - d2).abs <= p.p
}

trait StatsLibSpec[M[+_]] extends Specification
    with EvaluatorTestSupport[M]
    with StatsLib[M]
    with InfixLib[M]
    with MemoryDatasetConsumer[M]{ self =>
      
  import Function._
  
  import dag._
  import instructions._

  val testAPIKey = "testAPIKey"

  def testEval(graph: DepGraph): Set[SEvent] = withContext { ctx =>
    consumeEval(testAPIKey, graph, ctx, Path.Root) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  implicit def add_~=(d: Double) = new AlmostEqual(d)
  implicit val precision = Precision(0.000000001)

  "homogenous sets" should {
    "median with odd number of elements" >> {
      val line = Line(0, "")
      
      val input = dag.Morph1(line, Median,
        dag.LoadLocal(line, Const(line, CString("/hom/numbers"))))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d
      }
      
      result2 must contain(13)
    }
    
    "median with even number of elements" >> {
      val line = Line(0, "")
      
      val input = dag.Morph1(line, Median,
        dag.LoadLocal(line, Const(line, CString("/hom/numbers5"))))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d
      }
      
      result2 must contain(2)
    }

    "median with singleton" >> {
      val line = Line(0, "")
      
      val input = dag.Morph1(line, Median,
        Const(line, CLong(42)))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d
      }
      
      result2 must contain(42)
    }
    
    "mode" >> {
      val line = Line(0, "")
      
      val input = dag.Morph1(line, Mode,
        dag.LoadLocal(line, Const(line, CString("/hom/numbers2"))))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SArray(d)) if ids.length == 0 => d
      }
      
      result2 must contain(Vector(SDecimal(1)))
    }.pendingUntilFixed

    "mode with a singleton" >> {
      val line = Line(0, "")
      
      val input = dag.Morph1(line, Mode,
        Const(line, CLong(42)))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SArray(d)) if ids.length == 0 => d
      }
      
      result2 must contain(Vector(SDecimal(42)))
    }.pendingUntilFixed

    "mode where each value appears exactly once" >> {
      val line = Line(0, "")
      
      val input = dag.Morph1(line, Mode,
        dag.LoadLocal(line, Const(line, CString("/hom/numbers"))))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SArray(d)) if ids.length == 0 => d
      }
      
      result2 must contain(Vector(SDecimal(1), SDecimal(12), SDecimal(13), SDecimal(42), SDecimal(77)))
    }.pendingUntilFixed
    
    "compute rank" in {
      val line = Line(0, "")

      val input = dag.Morph1(line, Rank,
        dag.LoadLocal(line, Const(line, CString("/hom/numbers6"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(1,3,4,5,8,9).only
    }

    "compute rank within a filter" in {
      val line = Line(0, "")

      val numbers = dag.LoadLocal(line, Const(line, CString("/hom/numbers6")))

      val input = Filter(line, IdentitySort,
        numbers,
        Join(line, Eq, CrossLeftSort,
          dag.Morph1(line, Rank, numbers),
          Const(line, CLong(5))))
        
      val result = testEval(input)

      result must haveSize(3)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(11).only
    }

    "compute rank resulting in a boolean set" in {
      val line = Line(0, "")

      val input = Join(line, Eq, CrossLeftSort,
        dag.Morph1(line, Rank,
          dag.LoadLocal(line, Const(line, CString("/hom/numbers6")))),
        Const(line, CLong(5)))
        
      val result = testEval(input)

      result must haveSize(10)

      val (tr, fls) = result partition {
        case (ids, STrue) if ids.length == 1 => true
        case (ids, SFalse) if ids.length == 1 => false
        case _ => false
      }

      tr.size mustEqual 3
      fls.size mustEqual 7
    }

    "compute rank within a join" in {
      val line = Line(0, "")

      val input = Join(line, Add, CrossLeftSort,
        dag.Morph1(line, Rank,
          dag.LoadLocal(line, Const(line, CString("/hom/numbers6")))),
        Const(line, CLong(2)))
        
      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(3,5,6,7,10,11).only  
    }

    "compute denseRank" in {
      val line = Line(0, "")

      val input = dag.Morph1(line, DenseRank,
        dag.LoadLocal(line, Const(line, CString("/hom/numbers6"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(1,2,3,4,5,6).only  
    }

    "compute denseRank within a filter" in {
      val line = Line(0, "")

      val input = Filter(line, IdentitySort,
        dag.LoadLocal(line, Const(line, CString("/hom/numbers6"))),
        Join(line, Eq, CrossLeftSort,
          dag.Morph1(line, DenseRank,
            dag.LoadLocal(line, Const(line, CString("/hom/numbers6")))),
          Const(line, CLong(4))))
        
      val result = testEval(input)

      result must haveSize(3)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(11)
    }

    "compute denseRank within a join" in {
      val line = Line(0, "")

      val input = Join(line, Add, CrossLeftSort,
        dag.Morph1(line, DenseRank,
          dag.LoadLocal(line, Const(line, CString("/hom/numbers6")))),
        Const(line, CLong(2)))
        
      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(3,4,5,6,7,8).only  
    }

    "compute linear correlation" in {
      val line = Line(0, "")
      val heightWeight = dag.LoadLocal(line, Const(line, CString("hom/heightWeight")))
      
      val input = dag.Morph2(line, LinearCorrelation,
        Join(line, DerefObject, CrossLeftSort,
          heightWeight,
          Const(line, CString("height"))),
        Join(line, DerefObject, CrossLeftSort,
          heightWeight,
          Const(line, CString("weight"))))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 0.9998746737089123)
      }
      
      result2 must contain(true).only
    }

    "compute covariance" in {
      val line = Line(0, "")
      
      val input = dag.Morph2(line, Covariance,
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("hom/heightWeight"))),
          Const(line, CString("height"))),
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("hom/heightWeight"))),
          Const(line, CString("weight"))))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 400.08)
      }
      
      result2 must contain(true).only
    }

    "compute the correct coefficients in a simple linear regression" in {
      val line = Line(0, "")
      
      val input = dag.Morph2(line, LinearRegression,
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("hom/heightWeight"))),
          Const(line, CString("height"))),
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("hom/heightWeight"))),
          Const(line, CString("weight"))))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SObject(fields)) if ids.length == 0 => {
          val SDecimal(slope) = fields("slope")
          val SDecimal(yint) = fields("intercept")
          val bool1 = slope.toDouble ~= 0.6862906545903664
          val bool2 = yint.toDouble ~= 67.54013997529848
          Vector(bool1, bool2)
        }
      }
      
      result2 must contain(Vector(true, true))
    }

    "compute the correct coefficients in a simple log regression" in {
      val line = Line(0, "")
      
      val input = dag.Morph2(line, LogarithmicRegression,
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("hom/heightWeight"))),
          Const(line, CString("height"))),
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("hom/heightWeight"))),
          Const(line, CString("weight"))))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SObject(fields)) if ids.length == 0 => {
          val SDecimal(slope) = fields("slope")
          val SDecimal(yint) = fields("intercept")
          val bool1 = slope.toDouble ~= 38.8678597674246945
          val bool2 = yint.toDouble ~= -46.97865418113420686
          Vector(bool1, bool2)
        }
      }
      
      result2 must contain(Vector(true, true))
    }
  }   
  
  "heterogenous sets" should {
    "median" >> {
      val line = Line(0, "")
      
      val input = dag.Morph1(line, Median,
        dag.LoadLocal(line, Const(line, CString("/het/numbers"))))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d
      }
      
      result2 must contain(13)
    }
    
    "mode in the case there is only one" >> {
      val line = Line(0, "")
      
      val input = dag.Morph1(line, Mode,
        dag.LoadLocal(line, Const(line, CString("/het/numbers2"))))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SArray(d)) if ids.length == 0 => d
      }
      
      result2 must contain(Vector(SDecimal(1)))
    }.pendingUntilFixed
    
    "mode in the case there is more than one" >> {
      val line = Line(0, "")
      
      val input = dag.Morph1(line, Mode,
        dag.LoadLocal(line, Const(line, CString("/het/random"))))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SArray(d)) if ids.length == 0 => d
      }
      
      result2 must contain(Vector(SDecimal(4), SString("a")))
    }.pendingUntilFixed
    
    "compute rank" in {
      val line = Line(0, "")

      val input = dag.Morph1(line, Rank,
        dag.LoadLocal(line, Const(line, CString("/het/numbers6"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(1,3,4,5,8,9).only  
    }

    "compute rank within an equals filter" in {
      val line = Line(0, "")

      val input = Filter(line, IdentitySort,
        dag.LoadLocal(line, Const(line, CString("/het/numbers6"))),
        Join(line, Eq, CrossLeftSort,
          dag.Morph1(line, Rank,
            dag.LoadLocal(line, Const(line, CString("/het/numbers6")))),
          Const(line, CLong(9))))
        
      val result = testEval(input)

      result must haveSize(2)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(34).only
    }

    "compute rank within another equals filter" in {
      val line = Line(0, "")

      val input = Filter(line, IdentitySort,
        dag.LoadLocal(line, Const(line, CString("/het/numbers6"))),
        Join(line, Eq, CrossLeftSort,
          dag.Morph1(line, Rank,
            dag.LoadLocal(line, Const(line, CString("/het/numbers6")))),
          Const(line, CLong(1))))
        
      val result = testEval(input)

      result must haveSize(2)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(-10).only

    }

    "compute rank within a less-than filter" in {
      val line = Line(0, "")

      val input = Filter(line, IdentitySort,
        dag.LoadLocal(line, Const(line, CString("/het/numbers6"))),
        Join(line, LtEq, CrossLeftSort,
          dag.Morph1(line, Rank,
            dag.LoadLocal(line, Const(line, CString("/het/numbers6")))),
          Const(line, CLong(5))))
        
      val result = testEval(input)

      result must haveSize(7)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(-10,0,5,11).only
    }

    "compute rank within a join" in {
      val line = Line(0, "")

      val input = Join(line, Add, CrossLeftSort,
        dag.Morph1(line, Rank,
          dag.LoadLocal(line, Const(line, CString("/het/numbers6")))),
        Const(line, CLong(2)))
        
      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(3,5,6,7,10,11).only  
    }

    "compute denseRank" in {
      val line = Line(0, "")

      val input = dag.Morph1(line, DenseRank,
        dag.LoadLocal(line, Const(line, CString("/het/numbers6"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(1,2,3,4,5,6).only  
    }

    "compute denseRank within an equals filter" in {
      val line = Line(0, "")

      val input = Filter(line, IdentitySort,
        dag.LoadLocal(line, Const(line, CString("/het/numbers6"))),
        Join(line, Eq, CrossLeftSort,
          dag.Morph1(line, DenseRank,
            dag.LoadLocal(line, Const(line, CString("/het/numbers6")))),
          Const(line, CLong(6))))
        
      val result = testEval(input)

      result must haveSize(2)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(34)
    }

    "compute denseRank within a less-than filter" in {
      val line = Line(0, "")

      val input = Filter(line, IdentitySort,
        dag.LoadLocal(line, Const(line, CString("/het/numbers6"))),
        Join(line, LtEq, CrossLeftSort,
          dag.Morph1(line, DenseRank,
            dag.LoadLocal(line, Const(line, CString("/het/numbers6")))),
          Const(line, CLong(5))))
        
      val result = testEval(input)

      result must haveSize(8)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(-10,0,5,11,12).only
    }

    "compute denseRank within a join" in {
      val line = Line(0, "")

      val input = Join(line, Add, CrossLeftSort,
        dag.Morph1(line, DenseRank,
          dag.LoadLocal(line, Const(line, CString("/het/numbers6")))),
        Const(line, CLong(2)))
        
      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(3,4,5,6,7,8).only  
    }

    "compute linear correlation" in {
      val line = Line(0, "")
      
      val input = dag.Morph2(line, LinearCorrelation,
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("het/heightWeight"))),
          Const(line, CString("height"))),
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("het/heightWeight"))),
          Const(line, CString("weight"))))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 0.9998746737089123)
      }
      
      result2 must contain(true).only 
    }

    "compute covariance" in {
      val line = Line(0, "")
      
      val input = dag.Morph2(line, Covariance,
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("het/heightWeight"))),
          Const(line, CString("height"))),
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("het/heightWeight"))),
          Const(line, CString("weight"))))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 400.08)
      }
      
      result2 must contain(true).only
    }

    "compute the correct coefficients in a simple linear regression" in {
      val line = Line(0, "")
      
      val input = dag.Morph2(line, LinearRegression,
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("het/heightWeight"))),
          Const(line, CString("height"))),
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("het/heightWeight"))),
          Const(line, CString("weight"))))

      val result = testEval(input)
      
      result must haveSize(1)
       
      val result2 = result collect {
        case (ids, SObject(fields)) if ids.length == 0 => {
          val SDecimal(slope) = fields("slope")
          val SDecimal(yint) = fields("intercept")
          val bool1 = slope.toDouble ~= 0.6862906545903664
          val bool2 = yint.toDouble ~= 67.54013997529848
          Vector(bool1, bool2)
        }
      }
      
      result2 must contain(Vector(true, true)).only
    }

    "compute the correct coefficients in a simple log regression" in {
      val line = Line(0, "")
      
      val input = dag.Morph2(line, LogarithmicRegression,
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("het/heightWeight"))),
          Const(line, CString("height"))),
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("het/heightWeight"))),
          Const(line, CString("weight"))))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SObject(fields)) if ids.length == 0 => {
          val SDecimal(slope) = fields("slope")
          val SDecimal(yint) = fields("intercept")
          val bool1 = slope.toDouble ~= 38.8678597674246945
          val bool2 = yint.toDouble ~= -46.97865418113420686
          Vector(bool1, bool2)
        }
      }
      
      result2 must contain(Vector(true, true))
    }
  } 

  
  "for homogenous sets, in a cross, the appropriate stats function" should {
    "compute linear correlation" in {
      val line = Line(0, "")
      
      val input = dag.Morph2(line, LinearCorrelation,
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("hom/heightWeight"))),
          Const(line, CString("height"))),
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("hom/heightWeight"))),
          Const(line, CString("weight"))))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 0.9998746737089123)
      }
      
      result2 must contain(true).only
    }

    "compute covariance" in {
      val line = Line(0, "")
      
      val input = dag.Morph2(line, Covariance,
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("hom/heightWeight"))),
          Const(line, CString("height"))),
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("hom/heightWeight"))),
          Const(line, CString("weight"))))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 400.08)
      }
      
      result2 must contain(true).only
    }

    "compute the correct coefficients in a simple linear regression" in {
      val line = Line(0, "")
      
      val input = dag.Morph2(line, LinearRegression,
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("hom/heightWeight"))),
          Const(line, CString("height"))),
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("hom/heightWeight"))),
          Const(line, CString("weight"))))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SObject(fields)) if ids.length == 0 => {
          val SDecimal(slope) = fields("slope")
          val SDecimal(yint) = fields("intercept")
          val bool1 = slope.toDouble ~= 0.6862906545903664
          val bool2 = yint.toDouble ~= 67.54013997529848
          Vector(bool1, bool2)
        }
      }
      
      result2 must contain(Vector(true, true))
    }

    "compute the correct coefficients in a simple log regression" in {
      val line = Line(0, "")
      
      val input = dag.Morph2(line, LogarithmicRegression,
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("hom/heightWeight"))),
          Const(line, CString("height"))),
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("hom/heightWeight"))),
          Const(line, CString("weight"))))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SObject(fields)) if ids.length == 0 => {
          val SDecimal(slope) = fields("slope")
          val SDecimal(yint) = fields("intercept")
          val bool1 = slope.toDouble ~= 38.8678597674246945
          val bool2 = yint.toDouble ~= -46.97865418113420686
          Vector(bool1, bool2)
        }
      }
      
      result2 must contain(Vector(true, true))
    }
  }    
  
  "for the same homogenous set, the appropriate stats function" should {
    "compute linear correlation" in {
      val line = Line(0, "")
      
      val input = dag.Morph2(line, LinearCorrelation,
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("hom/heightWeight"))),
          Const(line, CString("height"))),
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("hom/heightWeight"))),
          Const(line, CString("height"))))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 1)
      }
      
      result2 must contain(true).only //todo test this answer to a certain level of accuracy
    }

    "compute covariance" in {
      val line = Line(0, "")
      
      val input = dag.Morph2(line, Covariance,
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("hom/heightWeight"))),
          Const(line, CString("height"))),
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("hom/heightWeight"))),
          Const(line, CString("height"))))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 582.96)
      }
      
      result2 must contain(true).only
    }

    "compute the correct coefficients in a simple linear regression" in {
      val line = Line(0, "")
      
      val input = dag.Morph2(line, LinearRegression,
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("hom/heightWeight"))),
          Const(line, CString("height"))),
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("hom/heightWeight"))),
          Const(line, CString("height"))))

      val result = testEval(input)
      
      result must haveSize(1)

      val result2 = result collect {
        case (ids, SObject(fields)) if ids.length == 0 => {
          val SDecimal(slope) = fields("slope")
          val SDecimal(yint) = fields("intercept")
          val bool1 = slope.toDouble ~= 1
          val bool2 = yint.toDouble ~= 0
          Vector(bool1, bool2)
        }
      }
      
      result2 must contain(Vector(true, true)).only
    }

    "compute the correct coefficients in a simple log regression" in {
      val line = Line(0, "")
      
      val input = dag.Morph2(line, LogarithmicRegression,
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("hom/heightWeight"))),
          Const(line, CString("height"))),
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("hom/heightWeight"))),
          Const(line, CString("height"))))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SObject(fields)) if ids.length == 0 => {
          val SDecimal(slope) = fields("slope")
          val SDecimal(yint) = fields("intercept")
          val bool1 = slope.toDouble ~= 56.591540847739639
          val bool2 = yint.toDouble ~= -166.690263558904667
          Vector(bool1, bool2)
        }
      }
      
      result2 must contain(Vector(true, true))
    }
  }  
  

  "for a homogenous set and a value, the appropriate stats function" should {
    "compute linear correlation" in {
      "with value on the right" in {
        val line = Line(0, "")
        
        val input = dag.Morph2(line, LinearCorrelation,
          Join(line, DerefObject, CrossLeftSort,
            dag.LoadLocal(line, Const(line, CString("hom/heightWeight"))),
            Const(line, CString("height"))),
          Const(line, CLong(5)))

        val result = testEval(input)
        
        result must haveSize(0)
      }
    
      "with value on the left" in {
        val line = Line(0, "")
        
        val input = dag.Morph2(line, LinearCorrelation,
          Const(line, CLong(5)),
          Join(line, DerefObject, CrossLeftSort,
            dag.LoadLocal(line, Const(line, CString("hom/heightWeight"))),
            Const(line, CString("height"))))

        val result = testEval(input)
        
        result must haveSize(0)
      }
    }

    "compute covariance" in {
      val line = Line(0, "")
      
      val input = dag.Morph2(line, Covariance,
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("hom/heightWeight"))),
          Const(line, CString("height"))),
        Const(line, CLong(5)))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 0)
      }
      
      result2 must contain(true)
    }

    "compute the correct coefficients in a simple linear regression" in {
      val line = Line(0, "")
      
      val input = dag.Morph2(line, LinearRegression,
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("hom/heightWeight"))),
          Const(line, CString("height"))),
        Const(line, CLong(5)))

      val result = testEval(input)
      
      result must haveSize(1)
       
      val result2 = result collect {
        case (ids, SObject(fields)) if ids.length == 0 => {
          val SDecimal(slope) = fields("slope")
          val SDecimal(yint) = fields("intercept")
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
        
        val input = dag.Morph2(line, LogarithmicRegression,
          Join(line, DerefObject, CrossLeftSort,
            dag.LoadLocal(line, Const(line, CString("hom/heightWeight"))),
            Const(line, CString("height"))),
          Const(line, CLong(5)))

        val result = testEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (ids, SObject(fields)) if ids.length == 0 => {
            val SDecimal(slope) = fields("slope")
            val SDecimal(yint) = fields("intercept")
            val bool1 = slope.toDouble ~= 0
            val bool2 = yint.toDouble ~= 5
            Vector(bool1, bool2)
          }
        }
        
        result2 must contain(Vector(true, true))
      }

      "with a negative constant x-value" >> {
        val line = Line(0, "")
        
        val input = dag.Morph2(line, LogarithmicRegression,
          Operate(line, Neg, 
            Const(line, CLong(5))),
          Join(line, DerefObject, CrossLeftSort,
            dag.LoadLocal(line, Const(line, CString("hom/heightWeight"))),
            Const(line, CString("height"))))

        val result = testEval(input)

        val input2 = dag.LoadLocal(line, Const(line, CString("hom/heightWeight")))

        val result2 = testEval(input2)
        
        result must haveSize(0)
        result2 must haveSize(5)

      }

      "with a negative x-value in one object" >> {
        val line = Line(0, "")
        
        val input = dag.Morph2(line, LogarithmicRegression,
          Join(line, DerefObject, CrossLeftSort,
            dag.LoadLocal(line, Const(line, CString("hom/heightWeight_neg"))),
            Const(line, CString("height"))),
          Join(line, DerefObject, CrossLeftSort,
            dag.LoadLocal(line, Const(line, CString("hom/heightWeight_neg"))),
            Const(line, CString("weight"))))

        val result = testEval(input)
        
        result must haveSize(1) 
        
        val result2 = result collect {
          case (ids, SObject(fields)) if ids.length == 0 => {
            val SDecimal(slope) = fields("slope")
            val SDecimal(yint) = fields("intercept")
            val bool1 = slope.toDouble ~= 38.8678597674246945
            val bool2 = yint.toDouble ~= -46.97865418113420686
            Vector(bool1, bool2)
          }
        }
        
        result2 must contain(Vector(true, true))
      }
    }
  }

  "homogenous sets across two slice boundaries (22 elements)" should {
    "median with odd number of elements" >> {
      val line = Line(0, "")

      val input = dag.Morph1(line, Median,
        dag.LoadLocal(line, Const(line, CString("/hom/numbersAcrossSlices"))))

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d
      }

      result2 must contain(-1.5)
    }

    "median with even number of elements" >> {
      val line = Line(0, "")

      val input = dag.Morph1(line, Median,
        dag.LoadLocal(line, Const(line, CString("/hom/numbersAcrossSlices"))))

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d
      }

      result2 must contain(-1.5)
    }

    "median with singleton" >> {
      val line = Line(0, "")

      val input = dag.Morph1(line, Median,
        Const(line, CLong(42)))

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d
      }

      result2 must contain(42)
    }

    "mode" >> {
      val line = Line(0, "")

      val input = dag.Morph1(line, Mode,
        dag.LoadLocal(line, Const(line, CString("/hom/numbersAcrossSlices"))))

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SArray(d)) if ids.length == 0 => d
      }

      result2 must contain(Vector(SDecimal(1)))
    }.pendingUntilFixed

    "mode with a singleton" >> {
      val line = Line(0, "")

      val input = dag.Morph1(line, Mode,
        Const(line, CLong(42)))

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SArray(d)) if ids.length == 0 => d
      }

      result2 must contain(Vector(SDecimal(42)))
    }.pendingUntilFixed

    "mode where each value appears exactly once" >> {
      val line = Line(0, "")

      val input = dag.Morph1(line, Mode,
        dag.LoadLocal(line, Const(line, CString("/hom/numbersAcrossSlices"))))

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SArray(d)) if ids.length == 0 => d
      }

      result2 must contain(Vector(SDecimal(1), SDecimal(12), SDecimal(13), SDecimal(42), SDecimal(77)))
    }.pendingUntilFixed

    "compute rank" in {
      val line = Line(0, "")

      val input = dag.Morph1(line, Rank,
        dag.LoadLocal(line, Const(line, CString("/hom/numbersAcrossSlices"))))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(5,14,20,1,6,21,13,2,12,16,11,8,19,4,15).only
    }

    "compute rank within a filter" in {
      val line = Line(0, "")

      val numbers = dag.LoadLocal(line, Const(line, CString("/hom/numbersAcrossSlices")))

      val input = Filter(line, IdentitySort,
        numbers,
        Join(line, Eq, CrossLeftSort,
          dag.Morph1(line, Rank, numbers),
          Const(line, CLong(5))))

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(-9).only
    }

    "compute rank resulting in a boolean set" in {
      val line = Line(0, "")

      val input = Join(line, Eq, CrossLeftSort,
        dag.Morph1(line, Rank,
          dag.LoadLocal(line, Const(line, CString("/hom/numbersAcrossSlices")))),
        Const(line, CLong(5)))

      val result = testEval(input)

      result must haveSize(22)

      val (tr, fls) = result partition {
        case (ids, STrue) if ids.length == 1 => true
        case (ids, SFalse) if ids.length == 1 => false
        case _ => false
      }

      tr.size mustEqual 1
      fls.size mustEqual 21
    }

    "compute rank within a join" in {
      val line = Line(0, "")

      val input = Join(line, Add, CrossLeftSort,
        dag.Morph1(line, Rank,
          dag.LoadLocal(line, Const(line, CString("/hom/numbersAcrossSlices")))),
        Const(line, CLong(2)))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(10,14,6,21,13,17,22,7,3,18,16,23,8,4,15).only
    }

    "compute denseRank" in {
      val line = Line(0, "")

      val input = dag.Morph1(line, DenseRank,
        dag.LoadLocal(line, Const(line, CString("/hom/numbersAcrossSlices"))))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(5,10,14,1,6,9,13,2,12,7,3,11,8,4,15).only
    }

    "compute denseRank within a filter" in {
      val line = Line(0, "")

      val input = Filter(line, IdentitySort,
        dag.LoadLocal(line, Const(line, CString("/hom/numbersAcrossSlices"))),
        Join(line, Eq, CrossLeftSort,
          dag.Morph1(line, DenseRank,
            dag.LoadLocal(line, Const(line, CString("/hom/numbersAcrossSlices")))),
          Const(line, CLong(4))))

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(-9)
    }

    "compute denseRank within a join" in {
      val line = Line(0, "")

      val input = Join(line, Add, CrossLeftSort,
        dag.Morph1(line, DenseRank,
          dag.LoadLocal(line, Const(line, CString("/hom/numbersAcrossSlices")))),
        Const(line, CLong(2)))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(5,10,14,6,9,13,17,12,7,3,16,11,8,4,15).only
    }

    "compute covariance" in {
      val line = Line(0, "")
      
      val input = dag.Morph2(line, Covariance,
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("hom/heightWeightAcrossSlices"))),
          Const(line, CString("height"))),
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("hom/heightWeightAcrossSlices"))),
          Const(line, CString("weight"))))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 301.5)
      }
      
      result2 must contain(true).only
    }

    "compute linear correlation" in {
      val line = Line(0, "")
      
      val input = dag.Morph2(line, LinearCorrelation,
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("hom/heightWeightAcrossSlices"))),
          Const(line, CString("height"))),
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("hom/heightWeightAcrossSlices"))),
          Const(line, CString("weight"))))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 0.2832061115667535364)
      }
      
      result2 must contain(true).only 
    }

    "compute the correct coefficients in a simple linear regression" in {
      val line = Line(0, "")
      
      val input = dag.Morph2(line, LinearRegression,
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("hom/heightWeightAcrossSlices"))),
          Const(line, CString("height"))),
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("hom/heightWeightAcrossSlices"))),
          Const(line, CString("weight"))))

      val result = testEval(input)
      
      result must haveSize(1)
       
      val result2 = result collect {
        case (ids, SObject(fields)) if ids.length == 0 => {
          val SDecimal(slope) = fields("slope")
          val SDecimal(yint) = fields("intercept")
          val bool1 = slope.toDouble ~= 0.551488261704282626112
          val bool2 = yint.toDouble ~= 96.337568593067376154
          Vector(bool1, bool2)
        }
      }
      
      result2 must contain(Vector(true, true)).only
    }
    
    "compute the correct coefficients in a simple log regression" in {
      val line = Line(0, "")
      
      val input = dag.Morph2(line, LogarithmicRegression,
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("hom/heightWeightAcrossSlices"))),
          Const(line, CString("height"))),
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("hom/heightWeightAcrossSlices"))),
          Const(line, CString("weight"))))

      val result = testEval(input)
      
      result must haveSize(1)
       
      val result2 = result collect {
        case (ids, SObject(fields)) if ids.length == 0 => {
          val SDecimal(slope) = fields("slope")
          val SDecimal(yint) = fields("intercept")
          val bool1 = slope.toDouble ~= 35.885416368041469881
          val bool2 = yint.toDouble ~= -14.930463966129221
          Vector(bool1, bool2)
        }
      }
      
      result2 must contain(Vector(true, true)).only
    }
  }

  "heterogenous sets across two slice boundaries (22 elements)" should {
    "median" >> {
      val line = Line(0, "")

      val input = dag.Morph1(line, Median,
        dag.LoadLocal(line, Const(line, CString("/het/numbersAcrossSlices"))))

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d
      }

      result2 must contain(1)
    }

    "mode in the case there is only one" >> {
      val line = Line(0, "")

      val input = dag.Morph1(line, Mode,
        dag.LoadLocal(line, Const(line, CString("/het/numbersAcrossSlices"))))

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SArray(d)) if ids.length == 0 => d
      }

      result2 must contain(Vector(SDecimal(1)))
    }.pendingUntilFixed

    "mode in the case there is more than one" >> {
      val line = Line(0, "")

      val input = dag.Morph1(line, Mode,
        dag.LoadLocal(line, Const(line, CString("/het/numbersAcrossSlices"))))

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SArray(d)) if ids.length == 0 => d
      }

      result2 must contain(Vector(SDecimal(4), SString("a")))
    }.pendingUntilFixed

    "compute rank" in {
      val line = Line(0, "")

      val input = dag.Morph1(line, Rank,
        dag.LoadLocal(line, Const(line, CString("/het/numbersAcrossSlices"))))

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(5,1,9,2,7,3,8).only
    }

    "compute rank within an equals filter" in {
      val line = Line(0, "")

      val input = Filter(line, IdentitySort,
        dag.LoadLocal(line, Const(line, CString("/het/numbersAcrossSlices"))),
        Join(line, Eq, CrossLeftSort,
          dag.Morph1(line, Rank,
            dag.LoadLocal(line, Const(line, CString("/het/numbersAcrossSlices")))),
          Const(line, CLong(9))))

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(12).only
    }

    "compute rank within another equals filter" in {
      val line = Line(0, "")

      val input = Filter(line, IdentitySort,
        dag.LoadLocal(line, Const(line, CString("/het/numbersAcrossSlices"))),
        Join(line, Eq, CrossLeftSort,
          dag.Morph1(line, Rank,
            dag.LoadLocal(line, Const(line, CString("/het/numbersAcrossSlices")))),
          Const(line, CLong(1))))

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(-3).only

    }

    "compute rank within a less-than filter" in {
      val line = Line(0, "")

      val input = Filter(line, IdentitySort,
        dag.LoadLocal(line, Const(line, CString("/het/numbersAcrossSlices"))),
        Join(line, LtEq, CrossLeftSort,
          dag.Morph1(line, Rank,
            dag.LoadLocal(line, Const(line, CString("/het/numbersAcrossSlices")))),
          Const(line, CLong(5))))

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(0,-1,1,-3).only
    }

    "compute rank within a join" in {
      val line = Line(0, "")

      val input = Join(line, Add, CrossLeftSort,
        dag.Morph1(line, Rank,
          dag.LoadLocal(line, Const(line, CString("/het/numbersAcrossSlices")))),
        Const(line, CLong(2)))

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(5,10,9,7,3,11,4).only
    }

    "compute denseRank" in {
      val line = Line(0, "")

      val input = dag.Morph1(line, DenseRank,
        dag.LoadLocal(line, Const(line, CString("/het/numbersAcrossSlices"))))

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(5,1,6,2,7,3,4).only
    }

    "compute denseRank within an equals filter" in {
      val line = Line(0, "")

      val input = Filter(line, IdentitySort,
        dag.LoadLocal(line, Const(line, CString("/het/numbersAcrossSlices"))),
        Join(line, Eq, CrossLeftSort,
          dag.Morph1(line, DenseRank,
            dag.LoadLocal(line, Const(line, CString("/het/numbersAcrossSlices")))),
          Const(line, CLong(6))))

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(5)
    }

    "compute denseRank within a less-than filter" in {
      val line = Line(0, "")

      val input = Filter(line, IdentitySort,
        dag.LoadLocal(line, Const(line, CString("/het/numbersAcrossSlices"))),
        Join(line, LtEq, CrossLeftSort,
          dag.Morph1(line, DenseRank,
            dag.LoadLocal(line, Const(line, CString("/het/numbersAcrossSlices")))),
          Const(line, CLong(5))))

      val result = testEval(input)

      result must haveSize(7)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(0,-3,1,2,-1).only
    }

    "compute denseRank within a join" in {
      val line = Line(0, "")

      val input = Join(line, Add, CrossLeftSort,
        dag.Morph1(line, DenseRank,
          dag.LoadLocal(line, Const(line, CString("/het/numbersAcrossSlices")))),
        Const(line, CLong(2)))

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(5,6,9,7,3,8,4).only
    }

    "compute covariance" in {
      val line = Line(0, "")
      
      val input = dag.Morph2(line, Covariance,
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("het/heightWeightAcrossSlices"))),
          Const(line, CString("height"))),
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("het/heightWeightAcrossSlices"))),
          Const(line, CString("weight"))))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 874.2741666666666)
      }
      
      result2 must contain(true).only
    }

    "compute correlation of 0 when datasets are uncorrelated" in {
      val line = Line(0, "")
      
      val input = dag.Morph2(line, LinearCorrelation,
        Join(line, DerefArray, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("uncorrelated"))),
          Const(line, CLong(0))),
        Join(line, DerefArray, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("uncorrelated"))),
          Const(line, CLong(1))))

      val result = testEval(input)
      
      result must haveSize(1)
      result must haveAllElementsLike { case (ids, SDecimal(d)) if ids.length == 0 =>
        d.toDouble must_== 0.0
      }
    }

    "compute linear correlation" in {
      val line = Line(0, "")
      
      val input = dag.Morph2(line, LinearCorrelation,
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("het/heightWeightAcrossSlices"))),
          Const(line, CString("height"))),
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("het/heightWeightAcrossSlices"))),
          Const(line, CString("weight"))))

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 0.7835742008825)
      }
      
      result2 must contain(true).only 
    }

    "compute the correct coefficients in a simple linear regression" in {
      val line = Line(0, "")
      
      val input = dag.Morph2(line, LinearRegression,
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("het/heightWeightAcrossSlices"))),
          Const(line, CString("height"))),
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("het/heightWeightAcrossSlices"))),
          Const(line, CString("weight"))))

      val result = testEval(input)
      
      result must haveSize(1)
       
      val result2 = result collect {
        case (ids, SObject(fields)) if ids.length == 0 => {
          val SDecimal(slope) = fields("slope")
          val SDecimal(yint) = fields("intercept")
          val bool1 = slope.toDouble ~= 1.454654738762821849
          val bool2 = yint.toDouble ~= 27.0157291837095112508
          Vector(bool1, bool2)
        }
      }
      
      result2 must contain(Vector(true, true)).only
    }    
    
    "compute the correct coefficients in a simple log regression" in {
      val line = Line(0, "")
      
      val input = dag.Morph2(line, LogarithmicRegression,
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("het/heightWeightAcrossSlices"))),
          Const(line, CString("height"))),
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("het/heightWeightAcrossSlices"))),
          Const(line, CString("weight"))))

      val result = testEval(input)
      
      result must haveSize(1)
       
      val result2 = result collect {
        case (ids, SObject(fields)) if ids.length == 0 => {
          val SDecimal(slope) = fields("slope")
          val SDecimal(yint) = fields("intercept")
          val bool1 = slope.toDouble ~= 84.092713766496588959
          val bool2 = yint.toDouble ~= -220.42413606579986360
          Vector(bool1, bool2)
        }
      }
      
      result2 must contain(Vector(true, true)).only
    }
  }
}

object StatsLibSpec extends StatsLibSpec[test.YId] with test.YIdInstances
