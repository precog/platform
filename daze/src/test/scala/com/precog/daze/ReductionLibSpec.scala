package com.precog.daze

import org.specs2.mutable._

import memoization._
import com.precog.yggdrasil._

import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.list._
import Iteratee._

import com.precog.common.VectorCase
import com.precog.util.IdGen

class ReductionLibSpec extends Specification
  with Evaluator
  with StubOperationsAPI 
  with TestConfigComponent 
  with DiskIterableMemoizationComponent 
  with ReductionLib 
  with InfixLib
  with MemoryDatasetConsumer { self =>
  override type Dataset[α] = IterableDataset[α]
  override type Memoable[α] = Iterable[α]

  import Function._
  
  import dag._
  import instructions._

  object ops extends Ops 
  
  val testUID = "testUID"

  def testEval(graph: DepGraph): Set[SEvent] = withContext { ctx =>
    consumeEval(testUID, graph, ctx) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  "reduce homogeneous sets" >> {
    "count" >> {
      val line = Line(0, "")
      
      val input = dag.Reduce(line, BuiltInReduction(Count),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => d
      }
      
      result2 must contain(5)
    }
    
    "geometricMean" >> {
      val line = Line(0, "")
      
      val input = dag.Reduce(line, BuiltInReduction(GeometricMean),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => d.toDouble
      }
      
      result2 must contain(13.822064739747386)
    }
    
    "mean" >> {
      val line = Line(0, "")
      
      val input = dag.Reduce(line, BuiltInReduction(Mean),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => d
      }
      
      result2 must contain(29)
    }
    
    "median with odd number of elements" >> {
      val line = Line(0, "")
      
      val input = dag.Reduce(line, BuiltInReduction(Median),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => d
      }
      
      result2 must contain(13)
    }
    
    "median with even number of elements" >> {
      val line = Line(0, "")
      
      val input = dag.Reduce(line, BuiltInReduction(Median),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers5")), Het))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => d
      }
      
      result2 must contain(2)
    }      

    "median with singleton" >> {
      val line = Line(0, "")
      
      val input = dag.Reduce(line, BuiltInReduction(Median),
        Root(line, PushNum("42")))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => d
      }
      
      result2 must contain(42)
    }
    
    "mode" >> {
      val line = Line(0, "")
      
      val input = dag.Reduce(line, BuiltInReduction(Mode),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers2")), Het))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SArray(d)) => d
      }
      
      result2 must contain(Vector(SDecimal(1)))
    }      

    "mode with a singleton" >> {
      val line = Line(0, "")
      
      val input = dag.Reduce(line, BuiltInReduction(Mode),
        Root(line, PushNum("42")))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SArray(d)) => d
      }
      
      result2 must contain(Vector(SDecimal(42)))
    }

    "mode where each value appears exactly once" >> {
      val line = Line(0, "")
      
      val input = dag.Reduce(line, BuiltInReduction(Mode),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SArray(d)) => d
      }
      
      result2 must contain(Vector(SDecimal(1), SDecimal(12), SDecimal(13), SDecimal(42), SDecimal(77)))
    }
    
    "max" >> {
      val line = Line(0, "")
      
      val input = dag.Reduce(line, BuiltInReduction(Max),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => d
      }
      
      result2 must contain(77)
    }
    
    "min" >> {
      val line = Line(0, "")
      
      val input = dag.Reduce(line, BuiltInReduction(Min),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => d
      }
      
      result2 must contain(1)
    }
    
    "standard deviation" >> {
      val line = Line(0, "")
      
      val input = dag.Reduce(line, BuiltInReduction(StdDev),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => d.toDouble
      }
      
      result2 must contain(27.575351312358652)
    }
    
    "sum" >> {
      val line = Line(0, "")
      
      val input = dag.Reduce(line, BuiltInReduction(Sum),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(145)
    }

    "sumSq" >> {
      val line = Line(0, "")
      
      val input = dag.Reduce(line, BuiltInReduction(SumSq),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => d.toDouble
      }
      
      result2 must contain(8007)
    }

    "variance" >> {
      val line = Line(0, "")
      
      val input = dag.Reduce(line, BuiltInReduction(Variance),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => d.toDouble
      }
      
      result2 must contain(760.4)
    }

  }

  "reduce heterogeneous sets" >> {
    "count" >> {
      val line = Line(0, "")
      
      val input = dag.Reduce(line, BuiltInReduction(Count),
        dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => d
      }
      
      result2 must contain(10)
    }    
    
    "geometricMean" >> {
      val line = Line(0, "")
      
      val input = dag.Reduce(line, BuiltInReduction(GeometricMean),
        dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => d
      }
      
      result2 must contain(13.822064739747386)
    }
    
    "mean" >> {
      val line = Line(0, "")
      
      val input = dag.Reduce(line, BuiltInReduction(Mean),
        dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => d
      }
      
      result2 must contain(29)
    }
    
    "median" >> {
      val line = Line(0, "")
      
      val input = dag.Reduce(line, BuiltInReduction(Median),
        dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => d
      }
      
      result2 must contain(13)
    }
    
    "mode in the case there is only one" >> {
      val line = Line(0, "")
      
      val input = dag.Reduce(line, BuiltInReduction(Mode),
        dag.LoadLocal(line, None, Root(line, PushString("/het/numbers2")), Het))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SArray(d)) => d
      }
      
      result2 must contain(Vector(SDecimal(1)))
    }
    
    "mode in the case there is more than one" >> {
      val line = Line(0, "")
      
      val input = dag.Reduce(line, BuiltInReduction(Mode),
        dag.LoadLocal(line, None, Root(line, PushString("/het/random")), Het))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SArray(d)) => d
      }
      
      result2 must contain(Vector(SDecimal(4), SString("a")))
    }
    
    "max" >> {
      val line = Line(0, "")
      
      val input = dag.Reduce(line, BuiltInReduction(Max),
        dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => d
      }
      
      result2 must contain(77)
    }
    
    "min" >> {
      val line = Line(0, "")
      
      val input = dag.Reduce(line, BuiltInReduction(Min),
        dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => d
      }
      
      result2 must contain(1)
    }
    
    "standard deviation" >> {
      val line = Line(0, "")
      
      val input = dag.Reduce(line, BuiltInReduction(StdDev),
        dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => d.toDouble
      }
      
      result2 must contain(27.575351312358652)
    }
    
    "sum" >> {
      val line = Line(0, "")
      
      val input = dag.Reduce(line, BuiltInReduction(Sum),
        dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => d
      }
      
      result2 must contain(145)
    }      

    "sumSq" >> {
      val line = Line(0, "")
      
      val input = dag.Reduce(line, BuiltInReduction(SumSq),
        dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => d
      }
      
      result2 must contain(8007)
    } 

    "variance" >> {
      val line = Line(0, "")
      
      val input = dag.Reduce(line, BuiltInReduction(Variance),
        dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => d
      }
      
      result2 must contain(760.4)
    }
  }
}
