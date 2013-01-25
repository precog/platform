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

trait ReductionLibSpec[M[+_]] extends Specification
    with EvaluatorTestSupport[M]
    with ReductionLib[M]
    with StatsLib[M]
    with InfixLib[M]
    with LongIdMemoryDatasetConsumer[M] { self =>
      
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

  "reduce homogeneous sets" >> {
    "singleton count" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Reduce(Count, Const(CString("alpha"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d
      }
      
      result2 must contain(1)
    }   
    
    "count" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Reduce(Count,
        dag.LoadLocal(Const(CString("/hom/numbers"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d
      }
      
      result2 must contain(5)
    }
    
    "geometricMean" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Reduce(GeometricMean,
        dag.LoadLocal(Const(CString("/hom/numbers"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d.toDouble
      }
      
      result2 must contain(13.822064739747386)
    }
    
    "mean" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Reduce(Mean,
        dag.LoadLocal(Const(CString("/hom/numbers"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d
      }
      
      result2 must contain(29)
    }
    
    "max" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Reduce(Max,
        dag.LoadLocal(Const(CString("/hom/numbers"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d
      }
      
      result2 must contain(77)
    }
    
    "min" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Reduce(Min,
        dag.LoadLocal(Const(CString("/hom/numbers"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d
      }
      
      result2 must contain(1)
    }
    
    "standard deviation" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Reduce(StdDev,
        dag.LoadLocal(Const(CString("/hom/numbers"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d.toDouble
      }
      
      result2 must contain(27.575351312358652)
    }
    
    "sum a singleton" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Reduce(Sum, Const(CLong(18))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d.toInt
      }
      
      result2 must contain(18)
    }    
    
    "sum" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Reduce(Sum,
        dag.LoadLocal(Const(CString("/hom/numbers"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d.toInt
      }
      
      result2 must contain(145)
    }
    
    "sumSq" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Reduce(SumSq,
        dag.LoadLocal(Const(CString("/hom/numbers"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d.toDouble
      }
      
      result2 must contain(8007)
    }
    
    "variance" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Reduce(Variance,
        dag.LoadLocal(Const(CString("/hom/numbers"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d.toDouble
      }
      
      result2 must contain(760.4)
    }
    
    "forall" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Reduce(Forall,
        dag.IUI(true,
          Const(CBoolean(true))(line),
          Const(CBoolean(false))(line))(line))(line)
      
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SBoolean(b)) if ids.length == 0 => b
      }
      
      result2 must contain(false)
    }
    
    "exists" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Reduce(Exists,
        dag.IUI(true,
          Const(CBoolean(true))(line),
          Const(CBoolean(false))(line))(line))(line)
      
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SBoolean(b)) if ids.length == 0 => b
      }
      
      result2 must contain(true)
    }
  }

  "reduce heterogeneous sets" >> {
    "count" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Reduce(Count,
        dag.LoadLocal(Const(CString("/het/numbers"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d
      }
      
      result2 must contain(10)
    }    
    
    "geometricMean" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Reduce(GeometricMean,
        dag.LoadLocal(Const(CString("/het/numbers"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d
      }
      
      result2 must contain(13.822064739747386)
    }
    
    "mean" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Reduce(Mean,
        dag.LoadLocal(Const(CString("/het/numbers"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d
      }
      
      result2 must contain(29)
    }
    
    "max" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Reduce(Max,
        dag.LoadLocal(Const(CString("/het/numbers"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d
      }
      
      result2 must contain(77)
    }
    
    "min" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Reduce(Min,
        dag.LoadLocal(Const(CString("/het/numbers"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d
      }
      
      result2 must contain(1)
    }
    
    "standard deviation" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Reduce(StdDev,
        dag.LoadLocal(Const(CString("/het/numbers"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d.toDouble
      }
      
      result2 must contain(27.575351312358652)
    }
    
    "sum" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Reduce(Sum,
        dag.LoadLocal(Const(CString("/het/numbers"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d
      }
      
      result2 must contain(145)
    }      
  
    "sumSq" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Reduce(SumSq,
        dag.LoadLocal(Const(CString("/het/numbers"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d
      }
      
      result2 must contain(8007)
    } 
  
    "variance" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Reduce(Variance,
        dag.LoadLocal(Const(CString("/het/numbers"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d
      }
      
      result2 must contain(760.4)
    }
  }

  "reduce heterogeneous sets across two slice boundaries (22 elements)" >> {
    "count" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Reduce(Count,
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d
      }
      
      result2 must contain(22)
    }    
    
    "geometricMean" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Reduce(GeometricMean,
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d
      }
      
      result2 must contain(0)
    }
    
    "mean" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Reduce(Mean,
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d
      }
      
      result2 must contain(1.8888888888888888)
    }
    
    "max" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Reduce(Max,
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d
      }
      
      result2 must contain(12)
    }
    
    "min" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Reduce(Min,
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d
      }
      
      result2 must contain(-3)
    }
    
    "standard deviation" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Reduce(StdDev,
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d.toDouble
      }
      
      result2 must contain(4.121608220220312)
    }
    
    "sum" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Reduce(Sum,
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d
      }
      
      result2 must contain(17)
    }      
  
    "sumSq" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Reduce(SumSq,
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d
      }
      
      result2 must contain(185)
    } 
  
    "variance" >> {
      val line = Line(1, 1, "")
      
      val input = dag.Reduce(Variance,
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d
      }
      
      result2 must contain(16.987654320987655)
    }
  }
  
  "reduce homogeneous sets across two slice boundaries (22 elements)" >> {
    "count" >> {
      val line = Line(1, 1, "")
  
      val input = dag.Reduce(Count,
        dag.LoadLocal(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)
  
      val result = testEval(input)
  
      result must haveSize(1)
  
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d
      }
  
      result2 must contain(22)
    }
  
    "geometricMean" >> {
      val line = Line(1, 1, "")
  
      val input = dag.Reduce(GeometricMean,
        dag.LoadLocal(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)
  
      val result = testEval(input)
  
      result must haveSize(1)
  
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d
      }
  
      result2 must contain(0)
    }
  
    "mean" >> {
      val line = Line(1, 1, "")
  
      val input = dag.Reduce(Mean,
        dag.LoadLocal(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)
  
      val result = testEval(input)
  
      result must haveSize(1)
  
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d
      }
  
      result2 must contain(0.9090909090909090909090909090909091)
    }
  
    "max" >> {
      val line = Line(1, 1, "")
  
      val input = dag.Reduce(Max,
        dag.LoadLocal(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)
  
      val result = testEval(input)
  
      result must haveSize(1)
  
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d
      }
  
      result2 must contain(15)
    }
  
    "min" >> {
      val line = Line(1, 1, "")
  
      val input = dag.Reduce(Min,
        dag.LoadLocal(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)
  
      val result = testEval(input)
  
      result must haveSize(1)
  
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d
      }
  
      result2 must contain(-14)
    }
  
    "standard deviation" >> {
      val line = Line(1, 1, "")
  
      val input = dag.Reduce(StdDev,
        dag.LoadLocal(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)
  
      val result = testEval(input)
  
      result must haveSize(1)
  
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d.toDouble
      }
  
      result2 must contain(10.193175483934386)
    }
  
    "sum" >> {
      val line = Line(1, 1, "")
  
      val input = dag.Reduce(Sum,
        dag.LoadLocal(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)
  
      val result = testEval(input)
  
      result must haveSize(1)
  
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d
      }
  
      result2 must contain(20)
    }
  
    "sumSq" >> {
      val line = Line(1, 1, "")
  
      val input = dag.Reduce(SumSq,
        dag.LoadLocal(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)
  
      val result = testEval(input)
  
      result must haveSize(1)
  
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d
      }
  
      result2 must contain(2304)
    }
  
    "variance" >> {
      val line = Line(1, 1, "")
  
      val input = dag.Reduce(Variance,
        dag.LoadLocal(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)
  
      val result = testEval(input)
  
      result must haveSize(1)
  
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0  => d
      }
  
      result2 must contain(103.9008264462809917355371900826446)
    }
  }
}

object ReductionLibSpec extends ReductionLibSpec[test.YId] with test.YIdInstances
