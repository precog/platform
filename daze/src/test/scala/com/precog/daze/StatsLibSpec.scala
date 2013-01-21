package com.precog.daze

import org.specs2.mutable._

import com.precog.yggdrasil._
import com.precog.common.Path

import scalaz._
import scalaz.std.list._

import com.precog.util.IdGen
import com.precog.util.IOUtils

case class Precision(p: Double)
class AlmostEqual(d: Double) {
  def ~=(d2: Double)(implicit p: Precision) = (d - d2).abs <= p.p
}

trait StatsLibSpec[M[+_]] extends Specification
    with EvaluatorTestSupport[M] 
    with StatsLib[M]
    with InfixLib[M]
    with LongIdMemoryDatasetConsumer[M]{ self =>
      
  import Function._
  
  import dag._
  import instructions._

  val testAPIKey = "testAPIKey"

  def testEval(graph: DepGraph): Set[SEvent] = {
    consumeEval(testAPIKey, graph, Path.Root) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  implicit def add_~=(d: Double) = new AlmostEqual(d)
  implicit val precision = Precision(0.000000001)

  "homogenous sets" should {
    "median with odd number of elements" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Morph1(Median,
        dag.LoadLocal(Const(CString("/hom/numbers"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d
      }
      
      result2 must contain(13)
    }
    
    "median with even number of elements" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Morph1(Median,
        dag.LoadLocal(Const(CString("/hom/numbers5"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d
      }
      
      result2 must contain(2)
    }

    "median with singleton" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Morph1(Median,
        Const(CLong(42))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d
      }
      
      result2 must contain(42)
    }
    
    "mode" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Morph1(Mode,
        dag.LoadLocal(Const(CString("/hom/numbers2"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SArray(d)) if ids.length == 0 => d
      }
      
      result2 must contain(Vector(SDecimal(1)))
    }.pendingUntilFixed

    "mode with a singleton" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Morph1(Mode,
        Const(CLong(42))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SArray(d)) if ids.length == 0 => d
      }
      
      result2 must contain(Vector(SDecimal(42)))
    }.pendingUntilFixed

    "mode where each value appears exactly once" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Morph1(Mode,
        dag.LoadLocal(Const(CString("/hom/numbers"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SArray(d)) if ids.length == 0 => d
      }
      
      result2 must contain(Vector(SDecimal(1), SDecimal(12), SDecimal(13), SDecimal(42), SDecimal(77)))
    }.pendingUntilFixed
    
    "compute rank" in {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Rank,
        dag.LoadLocal(Const(CString("/hom/numbers6"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(1,3,4,5,8,9).only
    }

    "compute rank within a filter" in {
      val line = Line(1, 1, "")

      val numbers = dag.LoadLocal(Const(CString("/hom/numbers6"))(line))(line)

      val input = Filter(IdentitySort,
        numbers,
        Join(Eq, CrossLeftSort,
          dag.Morph1(Rank, numbers)(line),
          Const(CLong(5))(line))(line))(line)
        
      val result = testEval(input)

      result must haveSize(3)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(11).only
    }

    "compute rank resulting in a boolean set" in {
      val line = Line(1, 1, "")

      val input = Join(Eq, CrossLeftSort,
        dag.Morph1(Rank,
          dag.LoadLocal(Const(CString("/hom/numbers6"))(line))(line))(line),
        Const(CLong(5))(line))(line)
        
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
      val line = Line(1, 1, "")

      val input = Join(Add, CrossLeftSort,
        dag.Morph1(Rank,
          dag.LoadLocal(Const(CString("/hom/numbers6"))(line))(line))(line),
        Const(CLong(2))(line))(line)
        
      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(3,5,6,7,10,11).only  
    }

    "compute denseRank" in {
      val line = Line(1, 1, "")

      val input = dag.Morph1(DenseRank,
        dag.LoadLocal(Const(CString("/hom/numbers6"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(1,2,3,4,5,6).only  
    }

    "compute denseRank within a filter" in {
      val line = Line(1, 1, "")

      val input = Filter(IdentitySort,
        dag.LoadLocal(Const(CString("/hom/numbers6"))(line))(line),
        Join(Eq, CrossLeftSort,
          dag.Morph1(DenseRank,
            dag.LoadLocal(Const(CString("/hom/numbers6"))(line))(line))(line),
          Const(CLong(4))(line))(line))(line)
        
      val result = testEval(input)

      result must haveSize(3)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(11)
    }

    "compute denseRank within a join" in {
      val line = Line(1, 1, "")

      val input = Join(Add, CrossLeftSort,
        dag.Morph1(DenseRank,
          dag.LoadLocal(Const(CString("/hom/numbers6"))(line))(line))(line),
        Const(CLong(2))(line))(line)
        
      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(3,4,5,6,7,8).only  
    }

    "compute linear correlation" in {
      val line = Line(1, 1, "")
      val heightWeight = dag.LoadLocal(Const(CString("hom/heightWeight"))(line))(line)
      
      val input = dag.Morph2(LinearCorrelation,
        Join(DerefObject, CrossLeftSort,
          heightWeight,
          Const(CString("height"))(line))(line),
        Join(DerefObject, CrossLeftSort,
          heightWeight,
          Const(CString("weight"))(line))(line))(line)

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 0.9998746737089123)
      }
      
      result2 must contain(true).only
    }

    "compute covariance" in {
      val line = Line(1, 1, "")
      
      val input = dag.Morph2(Covariance,
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 400.08)
      }
      
      result2 must contain(true).only
    }

    "compute the correct coefficients in a simple linear regression" in {
      val line = Line(1, 1, "")
      
      val input = dag.Morph2(LinearRegression,
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

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
      val line = Line(1, 1, "")
      
      val input = dag.Morph2(LogarithmicRegression,
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

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
      val line = Line(1, 1, "")
      
      val input = dag.Morph1(Median,
        dag.LoadLocal(Const(CString("/het/numbers"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d
      }
      
      result2 must contain(13)
    }
    
    "mode in the case there is only one" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Morph1(Mode,
        dag.LoadLocal(Const(CString("/het/numbers2"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SArray(d)) if ids.length == 0 => d
      }
      
      result2 must contain(Vector(SDecimal(1)))
    }.pendingUntilFixed
    
    "mode in the case there is more than one" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Morph1(Mode,
        dag.LoadLocal(Const(CString("/het/random"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SArray(d)) if ids.length == 0 => d
      }
      
      result2 must contain(Vector(SDecimal(4), SString("a")))
    }.pendingUntilFixed
    
    "compute rank" in {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Rank,
        dag.LoadLocal(Const(CString("/het/numbers6"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(1,3,4,5,8,9).only  
    }

    "compute rank within an equals filter" in {
      val line = Line(1, 1, "")

      val input = Filter(IdentitySort,
        dag.LoadLocal(Const(CString("/het/numbers6"))(line))(line),
        Join(Eq, CrossLeftSort,
          dag.Morph1(Rank,
            dag.LoadLocal(Const(CString("/het/numbers6"))(line))(line))(line),
          Const(CLong(9))(line))(line))(line)
        
      val result = testEval(input)

      result must haveSize(2)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(34).only
    }

    "compute rank within another equals filter" in {
      val line = Line(1, 1, "")

      val input = Filter(IdentitySort,
        dag.LoadLocal(Const(CString("/het/numbers6"))(line))(line),
        Join(Eq, CrossLeftSort,
          dag.Morph1(Rank,
            dag.LoadLocal(Const(CString("/het/numbers6"))(line))(line))(line),
          Const(CLong(1))(line))(line))(line)
        
      val result = testEval(input)

      result must haveSize(2)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(-10).only

    }

    "compute rank within a less-than filter" in {
      val line = Line(1, 1, "")

      val input = Filter(IdentitySort,
        dag.LoadLocal(Const(CString("/het/numbers6"))(line))(line),
        Join(LtEq, CrossLeftSort,
          dag.Morph1(Rank,
            dag.LoadLocal(Const(CString("/het/numbers6"))(line))(line))(line),
          Const(CLong(5))(line))(line))(line)
        
      val result = testEval(input)

      result must haveSize(7)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(-10,0,5,11).only
    }

    "compute rank within a join" in {
      val line = Line(1, 1, "")

      val input = Join(Add, CrossLeftSort,
        dag.Morph1(Rank,
          dag.LoadLocal(Const(CString("/het/numbers6"))(line))(line))(line),
        Const(CLong(2))(line))(line)
        
      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(3,5,6,7,10,11).only  
    }

    "compute denseRank" in {
      val line = Line(1, 1, "")

      val input = dag.Morph1(DenseRank,
        dag.LoadLocal(Const(CString("/het/numbers6"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(1,2,3,4,5,6).only  
    }

    "compute denseRank within an equals filter" in {
      val line = Line(1, 1, "")

      val input = Filter(IdentitySort,
        dag.LoadLocal(Const(CString("/het/numbers6"))(line))(line),
        Join(Eq, CrossLeftSort,
          dag.Morph1(DenseRank,
            dag.LoadLocal(Const(CString("/het/numbers6"))(line))(line))(line),
          Const(CLong(6))(line))(line))(line)
        
      val result = testEval(input)

      result must haveSize(2)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(34)
    }

    "compute denseRank within a less-than filter" in {
      val line = Line(1, 1, "")

      val input = Filter(IdentitySort,
        dag.LoadLocal(Const(CString("/het/numbers6"))(line))(line),
        Join(LtEq, CrossLeftSort,
          dag.Morph1(DenseRank,
            dag.LoadLocal(Const(CString("/het/numbers6"))(line))(line))(line),
          Const(CLong(5))(line))(line))(line)
        
      val result = testEval(input)

      result must haveSize(8)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(-10,0,5,11,12).only
    }

    "compute denseRank within a join" in {
      val line = Line(1, 1, "")

      val input = Join(Add, CrossLeftSort,
        dag.Morph1(DenseRank,
          dag.LoadLocal(Const(CString("/het/numbers6"))(line))(line))(line),
        Const(CLong(2))(line))(line)
        
      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(3,4,5,6,7,8).only  
    }

    "compute linear correlation" in {
      val line = Line(1, 1, "")
      
      val input = dag.Morph2(LinearCorrelation,
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("het/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("het/heightWeight"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 0.9998746737089123)
      }
      
      result2 must contain(true).only 
    }

    "compute covariance" in {
      val line = Line(1, 1, "")
      
      val input = dag.Morph2(Covariance,
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("het/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("het/heightWeight"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 400.08)
      }
      
      result2 must contain(true).only
    }

    "compute the correct coefficients in a simple linear regression" in {
      val line = Line(1, 1, "")
      
      val input = dag.Morph2(LinearRegression,
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("het/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("het/heightWeight"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

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
      val line = Line(1, 1, "")
      
      val input = dag.Morph2(LogarithmicRegression,
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("het/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("het/heightWeight"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

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
      val line = Line(1, 1, "")
      
      val input = dag.Morph2(LinearCorrelation,
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 0.9998746737089123)
      }
      
      result2 must contain(true).only
    }

    "compute covariance" in {
      val line = Line(1, 1, "")
      
      val input = dag.Morph2(Covariance,
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 400.08)
      }
      
      result2 must contain(true).only
    }

    "compute the correct coefficients in a simple linear regression" in {
      val line = Line(1, 1, "")
      
      val input = dag.Morph2(LinearRegression,
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

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
      val line = Line(1, 1, "")
      
      val input = dag.Morph2(LogarithmicRegression,
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

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
      val line = Line(1, 1, "")
      
      val input = dag.Morph2(LinearCorrelation,
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line))(line)

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 1)
      }
      
      result2 must contain(true).only //todo test this answer to a certain level of accuracy
    }

    "compute covariance" in {
      val line = Line(1, 1, "")
      
      val input = dag.Morph2(Covariance,
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line))(line)

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 582.96)
      }
      
      result2 must contain(true).only
    }

    "compute the correct coefficients in a simple linear regression" in {
      val line = Line(1, 1, "")
      
      val input = dag.Morph2(LinearRegression,
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line))(line)

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
      val line = Line(1, 1, "")
      
      val input = dag.Morph2(LogarithmicRegression,
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line))(line)

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
        val line = Line(1, 1, "")
        
        val input = dag.Morph2(LinearCorrelation,
          Join(DerefObject, CrossLeftSort,
            dag.LoadLocal(Const(CString("hom/heightWeight"))(line))(line),
            Const(CString("height"))(line))(line),
          Const(CLong(5))(line))(line)

        val result = testEval(input)
        
        result must haveSize(0)
      }
    
      "with value on the left" in {
        val line = Line(1, 1, "")
        
        val input = dag.Morph2(LinearCorrelation,
          Const(CLong(5))(line),
          Join(DerefObject, CrossLeftSort,
            dag.LoadLocal(Const(CString("hom/heightWeight"))(line))(line),
            Const(CString("height"))(line))(line))(line)

        val result = testEval(input)
        
        result must haveSize(0)
      }
    }

    "compute covariance" in {
      val line = Line(1, 1, "")
      
      val input = dag.Morph2(Covariance,
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Const(CLong(5))(line))(line)

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 0)
      }
      
      result2 must contain(true)
    }

    "compute the correct coefficients in a simple linear regression" in {
      val line = Line(1, 1, "")
      
      val input = dag.Morph2(LinearRegression,
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Const(CLong(5))(line))(line)

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
        val line = Line(1, 1, "")
        
        val input = dag.Morph2(LogarithmicRegression,
          Join(DerefObject, CrossLeftSort,
            dag.LoadLocal(Const(CString("hom/heightWeight"))(line))(line),
            Const(CString("height"))(line))(line),
          Const(CLong(5))(line))(line)

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
        val line = Line(1, 1, "")
        
        val input = dag.Morph2(LogarithmicRegression,
          Operate(Neg, 
            Const(CLong(5))(line))(line),
          Join(DerefObject, CrossLeftSort,
            dag.LoadLocal(Const(CString("hom/heightWeight"))(line))(line),
            Const(CString("height"))(line))(line))(line)

        val result = testEval(input)

        val input2 = dag.LoadLocal(Const(CString("hom/heightWeight"))(line))(line)

        val result2 = testEval(input2)
        
        result must haveSize(0)
        result2 must haveSize(5)

      }

      "with a negative x-value in one object" >> {
        val line = Line(1, 1, "")
        
        val input = dag.Morph2(LogarithmicRegression,
          Join(DerefObject, CrossLeftSort,
            dag.LoadLocal(Const(CString("hom/heightWeight_neg"))(line))(line),
            Const(CString("height"))(line))(line),
          Join(DerefObject, CrossLeftSort,
            dag.LoadLocal(Const(CString("hom/heightWeight_neg"))(line))(line),
            Const(CString("weight"))(line))(line))(line)

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
      val line = Line(1, 1, "")

      val input = dag.Morph1(Median,
        dag.LoadLocal(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d
      }

      result2 must contain(-1.5)
    }

    "median with even number of elements" >> {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Median,
        dag.LoadLocal(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d
      }

      result2 must contain(-1.5)
    }

    "median with singleton" >> {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Median,
        Const(CLong(42))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d
      }

      result2 must contain(42)
    }

    "mode" >> {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Mode,
        dag.LoadLocal(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SArray(d)) if ids.length == 0 => d
      }

      result2 must contain(Vector(SDecimal(1)))
    }.pendingUntilFixed

    "mode with a singleton" >> {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Mode,
        Const(CLong(42))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SArray(d)) if ids.length == 0 => d
      }

      result2 must contain(Vector(SDecimal(42)))
    }.pendingUntilFixed

    "mode where each value appears exactly once" >> {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Mode,
        dag.LoadLocal(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SArray(d)) if ids.length == 0 => d
      }

      result2 must contain(Vector(SDecimal(1), SDecimal(12), SDecimal(13), SDecimal(42), SDecimal(77)))
    }.pendingUntilFixed

    "compute rank" in {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Rank,
        dag.LoadLocal(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(5,14,20,1,6,21,13,2,12,16,11,8,19,4,15).only
    }

    "compute rank within a filter" in {
      val line = Line(1, 1, "")

      val numbers = dag.LoadLocal(Const(CString("/hom/numbersAcrossSlices"))(line))(line)

      val input = Filter(IdentitySort,
        numbers,
        Join(Eq, CrossLeftSort,
          dag.Morph1(Rank, numbers)(line),
          Const(CLong(5))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(-9).only
    }

    "compute rank resulting in a boolean set" in {
      val line = Line(1, 1, "")

      val input = Join(Eq, CrossLeftSort,
        dag.Morph1(Rank,
          dag.LoadLocal(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line),
        Const(CLong(5))(line))(line)

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
      val line = Line(1, 1, "")

      val input = Join(Add, CrossLeftSort,
        dag.Morph1(Rank,
          dag.LoadLocal(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line),
        Const(CLong(2))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(10,14,6,21,13,17,22,7,3,18,16,23,8,4,15).only
    }

    "compute denseRank" in {
      val line = Line(1, 1, "")

      val input = dag.Morph1(DenseRank,
        dag.LoadLocal(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(5,10,14,1,6,9,13,2,12,7,3,11,8,4,15).only
    }

    "compute denseRank within a filter" in {
      val line = Line(1, 1, "")

      val input = Filter(IdentitySort,
        dag.LoadLocal(Const(CString("/hom/numbersAcrossSlices"))(line))(line),
        Join(Eq, CrossLeftSort,
          dag.Morph1(DenseRank,
            dag.LoadLocal(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line),
          Const(CLong(4))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(-9)
    }

    "compute denseRank within a join" in {
      val line = Line(1, 1, "")

      val input = Join(Add, CrossLeftSort,
        dag.Morph1(DenseRank,
          dag.LoadLocal(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line),
        Const(CLong(2))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(5,10,14,6,9,13,17,12,7,3,16,11,8,4,15).only
    }

    "compute covariance" in {
      val line = Line(1, 1, "")
      
      val input = dag.Morph2(Covariance,
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("hom/heightWeightAcrossSlices"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("hom/heightWeightAcrossSlices"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 301.5)
      }
      
      result2 must contain(true).only
    }

    "compute linear correlation" in {
      val line = Line(1, 1, "")
      
      val input = dag.Morph2(LinearCorrelation,
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("hom/heightWeightAcrossSlices"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("hom/heightWeightAcrossSlices"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 0.2832061115667535364)
      }
      
      result2 must contain(true).only 
    }

    "compute the correct coefficients in a simple linear regression" in {
      val line = Line(1, 1, "")
      
      val input = dag.Morph2(LinearRegression,
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("hom/heightWeightAcrossSlices"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("hom/heightWeightAcrossSlices"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

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
      val line = Line(1, 1, "")
      
      val input = dag.Morph2(LogarithmicRegression,
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("hom/heightWeightAcrossSlices"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("hom/heightWeightAcrossSlices"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

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
      val line = Line(1, 1, "")

      val input = dag.Morph1(Median,
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d
      }

      result2 must contain(1)
    }

    "mode in the case there is only one" >> {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Mode,
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SArray(d)) if ids.length == 0 => d
      }

      result2 must contain(Vector(SDecimal(1)))
    }.pendingUntilFixed

    "mode in the case there is more than one" >> {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Mode,
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SArray(d)) if ids.length == 0 => d
      }

      result2 must contain(Vector(SDecimal(4), SString("a")))
    }.pendingUntilFixed

    "compute rank" in {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Rank,
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(5,1,9,2,7,3,8).only
    }

    "compute rank within an equals filter" in {
      val line = Line(1, 1, "")

      val input = Filter(IdentitySort,
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line),
        Join(Eq, CrossLeftSort,
          dag.Morph1(Rank,
            dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line),
          Const(CLong(9))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(12).only
    }

    "compute rank within another equals filter" in {
      val line = Line(1, 1, "")

      val input = Filter(IdentitySort,
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line),
        Join(Eq, CrossLeftSort,
          dag.Morph1(Rank,
            dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line),
          Const(CLong(1))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(-3).only

    }

    "compute rank within a less-than filter" in {
      val line = Line(1, 1, "")

      val input = Filter(IdentitySort,
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line),
        Join(LtEq, CrossLeftSort,
          dag.Morph1(Rank,
            dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line),
          Const(CLong(5))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(0,-1,1,-3).only
    }

    "compute rank within a join" in {
      val line = Line(1, 1, "")

      val input = Join(Add, CrossLeftSort,
        dag.Morph1(Rank,
          dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line),
        Const(CLong(2))(line))(line)

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(5,10,9,7,3,11,4).only
    }

    "compute denseRank" in {
      val line = Line(1, 1, "")

      val input = dag.Morph1(DenseRank,
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(5,1,6,2,7,3,4).only
    }

    "compute denseRank within an equals filter" in {
      val line = Line(1, 1, "")

      val input = Filter(IdentitySort,
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line),
        Join(Eq, CrossLeftSort,
          dag.Morph1(DenseRank,
            dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line),
          Const(CLong(6))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(5)
    }

    "compute denseRank within a less-than filter" in {
      val line = Line(1, 1, "")

      val input = Filter(IdentitySort,
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line),
        Join(LtEq, CrossLeftSort,
          dag.Morph1(DenseRank,
            dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line),
          Const(CLong(5))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(7)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(0,-3,1,2,-1).only
    }

    "compute denseRank within a join" in {
      val line = Line(1, 1, "")

      val input = Join(Add, CrossLeftSort,
        dag.Morph1(DenseRank,
          dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line),
        Const(CLong(2))(line))(line)

      val result = testEval(input)

      result must haveSize(9)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(5,6,9,7,3,8,4).only
    }

    "compute covariance" in {
      val line = Line(1, 1, "")
      
      val input = dag.Morph2(Covariance,
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("het/heightWeightAcrossSlices"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("het/heightWeightAcrossSlices"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 874.2741666666666)
      }
      
      result2 must contain(true).only
    }

    "compute correlation of 0 when datasets are uncorrelated" in {
      val line = Line(1, 1, "")
      
      val input = dag.Morph2(LinearCorrelation,
        Join(DerefArray, CrossLeftSort,
          dag.LoadLocal(Const(CString("uncorrelated"))(line))(line),
          Const(CLong(0))(line))(line),
        Join(DerefArray, CrossLeftSort,
          dag.LoadLocal(Const(CString("uncorrelated"))(line))(line),
          Const(CLong(1))(line))(line))(line)

      val result = testEval(input)
      
      result must haveSize(1)
      result must haveAllElementsLike { case (ids, SDecimal(d)) if ids.length == 0 =>
        d.toDouble must_== 0.0
      }
    }

    "compute linear correlation" in {
      val line = Line(1, 1, "")
      
      val input = dag.Morph2(LinearCorrelation,
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("het/heightWeightAcrossSlices"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("het/heightWeightAcrossSlices"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 0.7835742008825)
      }
      
      result2 must contain(true).only 
    }

    "compute the correct coefficients in a simple linear regression" in {
      val line = Line(1, 1, "")
      
      val input = dag.Morph2(LinearRegression,
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("het/heightWeightAcrossSlices"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("het/heightWeightAcrossSlices"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

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
      val line = Line(1, 1, "")
      
      val input = dag.Morph2(LogarithmicRegression,
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("het/heightWeightAcrossSlices"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("het/heightWeightAcrossSlices"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

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
