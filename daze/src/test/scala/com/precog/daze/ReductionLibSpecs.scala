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

import com.precog.common._
import com.precog.yggdrasil._
import com.precog.common.Path

import scala.Function._
  
import scalaz._
import scalaz.std.list._

import com.precog.util.IdGen

trait ReductionLibSpecs[M[+_]] extends Specification
    with EvaluatorTestSupport[M]
    with LongIdMemoryDatasetConsumer[M] { self =>
      
  import dag._
  import instructions._
  import library._

  val testAPIKey = "testAPIKey"

  def testEval(graph: DepGraph): Set[SEvent] = {
    consumeEval(graph, defaultEvaluationContext) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  def determineResult(input: DepGraph, value: Double) = {
    val result = testEval(input)
    
    result must haveSize(1)
    
    val result2 = result collect {
      case (ids, SDecimal(d)) if ids.length == 0 => d.toDouble
    }
    
    result2 must contain(value)
  }

  val line = Line(1, 1, "")

  def parseDateTimeFuzzy(time: String) =
    Operate(BuiltInFunction1Op(ParseDateTimeFuzzy), 
      dag.LoadLocal(Const(CString(time))(line))(line))(line)

  "reduce homogeneous sets" >> {
    "singleton count" >> {
      val input = dag.Reduce(Count, Const(CString("alpha"))(line))(line)
        
      determineResult(input, 1)
    }   
    
    "count" >> {
      val input = dag.Reduce(Count,
        dag.LoadLocal(Const(CString("/hom/numbers"))(line))(line))(line)

      determineResult(input, 5)
    }
    
    "geometricMean" >> {
      val input = dag.Reduce(GeometricMean,
        dag.LoadLocal(Const(CString("/hom/numbers"))(line))(line))(line)
        
      determineResult(input, 13.822064739747386)
    }
    
    "mean" >> {
      val input = dag.Reduce(Mean,
        dag.LoadLocal(Const(CString("/hom/numbers"))(line))(line))(line)
        
      determineResult(input, 29)
    }
    
    "max" >> {
      val input = dag.Reduce(Max,
        dag.LoadLocal(Const(CString("/hom/numbers"))(line))(line))(line)
        
      determineResult(input, 77)
    }

    "min" >> {
      val input = dag.Reduce(Min,
        dag.LoadLocal(Const(CString("/hom/numbers"))(line))(line))(line)

      determineResult(input, 1)
    }

    "maxTime" >> {
      val input = dag.Reduce(MaxTime,
        parseDateTimeFuzzy("/hom/iso8601"))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 0 => d
      }
      
      result2 must contain("2012-12-28T22:38:19.430+06:00")
    }

    "minTime" >> {
      val input = dag.Reduce(MinTime,
        parseDateTimeFuzzy("/hom/iso8601"))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 0 => d
      }
      
      result2 must contain("2010-04-29T09:37:52.599+08:00")
    }
    
    "standard deviation" >> {
      val input = dag.Reduce(StdDev,
        dag.LoadLocal(Const(CString("/hom/numbers"))(line))(line))(line)
        
      determineResult(input, 27.575351312358652)
    }
    
    "sum a singleton" >> {
      val input = dag.Reduce(Sum, Const(CLong(18))(line))(line)
        
      determineResult(input, 18)
    }    
    
    "sum" >> {
      val input = dag.Reduce(Sum,
        dag.LoadLocal(Const(CString("/hom/numbers"))(line))(line))(line)
        
      determineResult(input, 145)
    }
    
    "sumSq" >> {
      val input = dag.Reduce(SumSq,
        dag.LoadLocal(Const(CString("/hom/numbers"))(line))(line))(line)
        
      determineResult(input, 8007)
    }
    
    "variance" >> {
      val input = dag.Reduce(Variance,
        dag.LoadLocal(Const(CString("/hom/numbers"))(line))(line))(line)
        
      determineResult(input, 760.4)
    }
    
    "forall" >> {
      val input = dag.Reduce(Forall,
        dag.IUI(true,
          Const(CTrue)(line),
          Const(CFalse)(line))(line))(line)
      
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SBoolean(b)) if ids.length == 0 => b
      }
      
      result2 must contain(false)
    }
    
    "exists" >> {
      val input = dag.Reduce(Exists,
        dag.IUI(true,
          Const(CTrue)(line),
          Const(CFalse)(line))(line))(line)
      
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
      val input = dag.Reduce(Count,
        dag.LoadLocal(Const(CString("/het/numbers"))(line))(line))(line)

      determineResult(input, 10)
    }    
    
    "geometricMean" >> {
      val input = dag.Reduce(GeometricMean,
        dag.LoadLocal(Const(CString("/het/numbers"))(line))(line))(line)

      determineResult(input, 13.822064739747386)
    }
    
    "mean" >> {
      val input = dag.Reduce(Mean,
        dag.LoadLocal(Const(CString("/het/numbers"))(line))(line))(line)
        
      determineResult(input, 29)
    }
    
    "max" >> {
      val input = dag.Reduce(Max,
        dag.LoadLocal(Const(CString("/het/numbers"))(line))(line))(line)
        
      determineResult(input, 77)
    }
    
    "min" >> {
      val input = dag.Reduce(Min,
        dag.LoadLocal(Const(CString("/het/numbers"))(line))(line))(line)
        
      determineResult(input, 1)
    }

    "maxTime" >> {
      val input = dag.Reduce(MaxTime,
        parseDateTimeFuzzy("/het/iso8601"))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 0 => d
      }
      
      result2 must contain("2012-12-28T22:38:19.430+06:00")
    }

    "minTime" >> {
      val input = dag.Reduce(MinTime,
        parseDateTimeFuzzy("/het/iso8601"))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 0 => d
      }
      
      result2 must contain("2010-04-29T09:37:52.599+08:00")
    }
    
    "standard deviation" >> {
      val input = dag.Reduce(StdDev,
        dag.LoadLocal(Const(CString("/het/numbers"))(line))(line))(line)
        
      determineResult(input, 27.575351312358652)
    }
    
    "sum" >> {
      val input = dag.Reduce(Sum,
        dag.LoadLocal(Const(CString("/het/numbers"))(line))(line))(line)
        
      determineResult(input, 145)
    }      
  
    "sumSq" >> {
      val input = dag.Reduce(SumSq,
        dag.LoadLocal(Const(CString("/het/numbers"))(line))(line))(line)
        
      determineResult(input, 8007)
    } 
  
    "variance" >> {
      val input = dag.Reduce(Variance,
        dag.LoadLocal(Const(CString("/het/numbers"))(line))(line))(line)
        
      determineResult(input, 760.4)
    }
  }

  "reduce heterogeneous sets across two slice boundaries (22 elements)" >> {
    "count" >> {
      val input = dag.Reduce(Count,
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)
        
      determineResult(input, 22)
    }    
    
    "geometricMean" >> {
      val input = dag.Reduce(GeometricMean,
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)
        
      determineResult(input, 0)
    }
    
    "mean" >> {
      val input = dag.Reduce(Mean,
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)
        
      determineResult(input, 1.8888888888888888)
    }
    
    "max" >> {
      val input = dag.Reduce(Max,
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      determineResult(input, 12) 
    }
    
    "min" >> {
      val input = dag.Reduce(Min,
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)
        
      determineResult(input, -3) 
    }

    "maxTime" >> {
      val input = dag.Reduce(MaxTime,
        parseDateTimeFuzzy("/hom/iso8601AcrossSlices"))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 0 => d
      }
      
      result2 must contain("2012-12-28T22:38:19.430+06:00")
    }

    "minTime" >> {
      val input = dag.Reduce(MinTime,
        parseDateTimeFuzzy("/hom/iso8601AcrossSlices"))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 0 => d
      }
      
      result2 must contain("2007-02-04T10:58:14.041-01:00")
    }
    
    "standard deviation" >> {
      val input = dag.Reduce(StdDev,
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)
        
      determineResult(input, 4.121608220220312) 
    }
    
    "sum" >> {
      val input = dag.Reduce(Sum,
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)
        
      determineResult(input, 17) 
    }      
  
    "sumSq" >> {
      val input = dag.Reduce(SumSq,
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)
        
      determineResult(input, 185) 
    } 
  
    "variance" >> {
      val input = dag.Reduce(Variance,
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)
        
      determineResult(input, 16.987654320987655) 
    }
  }
  
  "reduce homogeneous sets across two slice boundaries (22 elements)" >> {
    "count" >> {
      val input = dag.Reduce(Count,
        dag.LoadLocal(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)
  
      determineResult(input, 22) 
    }
  
    "geometricMean" >> {
      val input = dag.Reduce(GeometricMean,
        dag.LoadLocal(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)
  
      determineResult(input, 0) 
    }
  
    "mean" >> {
      val input = dag.Reduce(Mean,
        dag.LoadLocal(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)
  
      determineResult(input, 0.9090909090909090909090909090909091) 
    }
  
    "max" >> {
      val input = dag.Reduce(Max,
        dag.LoadLocal(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)
  
      determineResult(input, 15) 
    }
  
    "min" >> {
      val input = dag.Reduce(Min,
        dag.LoadLocal(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)
  
      determineResult(input, -14) 
    }

    "maxTime" >> {
      val input = dag.Reduce(MaxTime,
        parseDateTimeFuzzy("/het/iso8601AcrossSlices"))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 0 => d
      }
      
      result2 must contain("2012-05-05T08:58:10.171+10:00")
    }

    "minTime" >> {
      val input = dag.Reduce(MinTime,
        parseDateTimeFuzzy("/het/iso8601AcrossSlices"))(line)
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 0 => d
      }
      
      result2 must contain("2007-07-14T03:49:30.311-07:00")
    }
  
    "standard deviation" >> {
      val input = dag.Reduce(StdDev,
        dag.LoadLocal(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)
  
      determineResult(input, 10.193175483934386) 
    }
  
    "sum" >> {
      val input = dag.Reduce(Sum,
        dag.LoadLocal(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)
  
      determineResult(input, 20) 
    }
  
    "sumSq" >> {
      val input = dag.Reduce(SumSq,
        dag.LoadLocal(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)
  
      determineResult(input, 2304) 
    }
  
    "variance" >> {
      val input = dag.Reduce(Variance,
        dag.LoadLocal(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)
  
      determineResult(input, 103.9008264462809917355371900826446) 
    }
  }
}

object ReductionLibSpecs extends ReductionLibSpecs[test.YId] with test.YIdInstances
