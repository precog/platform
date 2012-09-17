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
import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.list._
import Iteratee._

import com.precog.util.IdGen

trait MathLibSpec[M[+_]] extends Specification
    with EvaluatorTestSupport[M]
    with MathLib[M] 
    with MemoryDatasetConsumer[M] { self =>
      
  import Function._
  
  import dag._
  import instructions._

  val testUID = "testUID"

  def testEval(graph: DepGraph): Set[SEvent] = withContext { ctx =>
    consumeEval(testUID, graph, ctx, Path.Root) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  "for sets with numeric values inside arrays and objects" should {
    "compute cos only of the numeric value" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(cos),
        dag.LoadLocal(line, Root(line, PushString("/het/numbers7"))))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(1)
    }
  }

  "for homogeneous sets, the appropriate math function" should {   //todo test in particular cases when functions are not defined!!
    "compute sinh" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(sinh),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0, 1.1752011936438014, -1.1752011936438014, 8.696374707602505E17, -4.872401723124452E9)
    }     
    "compute sinh on two large(ish) values" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(sinh),
        dag.LoadLocal(line, Root(line, PushString("/hom/number"))))
        
      val result = testEval(input)
      
      result must haveSize(0)
    }  
    "compute toDegrees" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(toDegrees),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 57.29577951308232, -57.29577951308232, 2406.4227395494577, -1317.8029288008934)
    }  
    "compute expm1" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(expm1),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 1.718281828459045, -0.6321205588285577, 1.73927494152050099E18, -0.9999999998973812)
    }      
    "compute expm1 on two large(ish) values" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(expm1),
        dag.LoadLocal(line, Root(line, PushString("/hom/number"))))
        
      val result = testEval(input)
      
      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(-1.0)
    }  
    "compute getExponent" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(getExponent),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(3)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0, 5)
    }  
    "compute asin" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(asin),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 1.5707963267948966, -1.5707963267948966)
    }  
    "compute log10" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(log10),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(3)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 1.6232492903979006)
    }  
    "compute cos" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(cos),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(1.0, 0.5403023058681398, 0.5403023058681398, -0.39998531498835127, -0.5328330203333975)
    }  
    "compute exp" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(exp),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(1.0, 2.7182818284590455, 0.36787944117144233, 1.73927494152050099E18, 1.026187963170189E-10)
    }  
    "compute exp on two large(ish) values" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(exp),
        dag.LoadLocal(line, Root(line, PushString("/hom/number"))))
        
      val result = testEval(input)
      
      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0)
    } 
    "compute cbrt" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(cbrt),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 1.0, -1.0, 3.4760266448864496, -2.8438669798515654)
    }  
    "compute atan" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(atan),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 0.7853981633974483, -0.7853981633974483, 1.5469913006098266, -1.5273454314033659)
    }  
    "compute ceil" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(ceil),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 1.0, -1.0, 42.0, -23.0)
    }  
    "compute rint" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(rint),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 1.0, -1.0, 42.0, -23.0)
    }  
    "compute log1p" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(log1p),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 0.6931471805599453, 3.7612001156935624)
    }  
    "compute sqrt" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(sqrt),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 1.0, 6.48074069840786)
    }  
    "compute floor" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(floor),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 1.0, -1.0, 42.0, -23.0)
    }  
    "compute toRadians" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(toRadians),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 0.017453292519943295, -0.017453292519943295, 0.7330382858376184, -0.40142572795869574)
    }  
    "compute tanh" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(tanh),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 0.7615941559557649, -0.7615941559557649, 1.0, -1.0)
    }  
    "compute round" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(round),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0, 1, -1, 42, -23)
    }  
    "compute cosh" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(cosh),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(1.0, 1.543080634815244, 1.543080634815244, 8.696374707602505E17, 4.872401723124452E9)
    }  
    "compute cosh on two large(ish) values" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(cosh),
        dag.LoadLocal(line, Root(line, PushString("/hom/number"))))
        
      val result = testEval(input)
      
      result must haveSize(0)
    } 
    "compute tan" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(tan),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 1.5574077246549023, -1.5574077246549023, 2.2913879924374863, -1.5881530833912738)
    }  
    "compute abs" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(abs),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0, 1, 42, 23)
    }  
    "compute sin" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(sin),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 0.8414709848078965, -0.8414709848078965, -0.9165215479156338, 0.8462204041751706)
    }  
    "compute log" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(log),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(3)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 3.7376696182833684)
    }  
    "compute signum" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(signum),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 1.0, -1.0)
    }  
    "compute acos" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(acos),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(1.5707963267948966, 0.0, 3.141592653589793)
    }  
    "compute ulp" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(ulp),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(4.9E-324, 2.220446049250313E-16, 2.220446049250313E-16, 7.105427357601002E-15, 3.552713678800501E-15)
    }
    "compute min" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(min), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))),
        Root(line, PushNum("7")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0, 1, -1, 7, -23)
    }
    "compute hypot" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(hypot), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))),
        Root(line, PushNum("7")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(7.0, 7.0710678118654755, 7.0710678118654755, 42.579337712087536, 24.041630560342615)
    }
    "compute pow" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(pow), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))),
        Root(line, PushNum("7")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 1.0, -1.0, 2.30539333248E11, -3.404825447E9)
    }
    "compute max" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(max), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))),
        Root(line, PushNum("7")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(7, 42)
    }
    "compute atan2" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(atan2), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))),
        Root(line, PushNum("7")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 0.1418970546041639, -0.1418970546041639, 1.4056476493802699, -1.2753554896511767)
    }
    "compute copySign" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(copySign), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))),
        Root(line, PushNum("7")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 1.0, 42.0, 23.0)
    }
    "compute IEEEremainder" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(IEEEremainder), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))),
        Root(line, PushNum("7")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 1.0, -1.0, -2.0)
    }
  }

  "for heterogeneous sets, the appropriate math function" should {
    "compute sinh" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(sinh),
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0, 1.1752011936438014, -1.1752011936438014, 8.696374707602505E17, -4.872401723124452E9)
    }  
    "compute toDegrees" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(toDegrees),
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 57.29577951308232, -57.29577951308232, 2406.4227395494577, -1317.8029288008934)
    }  
    "compute expm1" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(expm1),
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 1.718281828459045, -0.6321205588285577, 1.73927494152050099E18, -0.9999999998973812)
    }  
    "compute getExponent" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(getExponent),
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(3)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0, 5)
    }  
    "compute asin" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(asin),
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 1.5707963267948966, -1.5707963267948966)
    }  
    "compute log10" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(log10),
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(3)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 1.6232492903979006)
    }  
    "compute cos" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(cos),
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(1.0, 0.5403023058681398, 0.5403023058681398, -0.39998531498835127, -0.5328330203333975)
    }  
    "compute exp" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(exp),
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(1.0, 2.7182818284590455, 0.36787944117144233, 1.73927494152050099E18, 1.026187963170189E-10)
    }  
    "compute cbrt" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(cbrt),
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 1.0, -1.0, 3.4760266448864496, -2.8438669798515654)
    }  
    "compute atan" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(atan),
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 0.7853981633974483, -0.7853981633974483, 1.5469913006098266, -1.5273454314033659)
    }  
    "compute ceil" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(ceil),
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 1.0, -1.0, 42.0, -23.0)
    }  
    "compute rint" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(rint),
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 1.0, -1.0, 42.0, -23.0)
    }  
    "compute log1p" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(log1p),
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 0.6931471805599453, 3.7612001156935624)
    }  
    "compute sqrt" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(sqrt),
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 1.0, 6.48074069840786)
    }  
    "compute floor" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(floor),
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 1.0, -1.0, 42.0, -23.0)
    }  
    "compute toRadians" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(toRadians),
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 0.017453292519943295, -0.017453292519943295, 0.7330382858376184, -0.40142572795869574)
    }  
    "compute tanh" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(tanh),
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 0.7615941559557649, -0.7615941559557649, 1.0, -1.0)
    }  
    "compute round" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(round),
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0, 1, -1, 42, -23)
    }  
    "compute cosh" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(cosh),
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(1.0, 1.543080634815244, 1.543080634815244, 8.696374707602505E17, 4.872401723124452E9)
    }  
    "compute tan" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(tan),
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 1.5574077246549023, -1.5574077246549023, 2.2913879924374863, -1.5881530833912738)
    }  
    "compute abs" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(abs),
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0, 1, 42, 23)
    }  
    "compute sin" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(sin),
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 0.8414709848078965, -0.8414709848078965, -0.9165215479156338, 0.8462204041751706)
    }  
    "compute log" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(log),
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(3)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 3.7376696182833684)
    }  
    "compute signum" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(signum),
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 1.0, -1.0)
    }  
    "compute acos" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(acos),
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(1.5707963267948966, 0.0, 3.141592653589793)
    }  
    "compute ulp" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(ulp),
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(4.9E-324, 2.220446049250313E-16, 2.220446049250313E-16, 7.105427357601002E-15, 3.552713678800501E-15)
    }
    "compute min" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(min), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))),
        Root(line, PushNum("7")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0, 1, -1, 7, -23)
    }
    "compute hypot" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(hypot), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))),
        Root(line, PushNum("7")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(7.0, 7.0710678118654755, 7.0710678118654755, 42.579337712087536, 24.041630560342615)
    }
    "compute pow" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(pow), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))),
        Root(line, PushNum("7")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 1.0, -1.0, 2.30539333248E11, -3.404825447E9)
    }
    "compute max" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(max), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))),
        Root(line, PushNum("7")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(7, 42)
    }
    "compute atan2" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(atan2), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))),
        Root(line, PushNum("7")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 0.1418970546041639, -0.1418970546041639, 1.4056476493802699, -1.2753554896511767)
    }
    "compute copySign" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(copySign), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))),
        Root(line, PushNum("7")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 1.0, 42.0, 23.0)
    }
    "compute IEEEremainder" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(IEEEremainder), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))),
        Root(line, PushNum("7")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1  => d
      }
      
      result2 must contain(0.0, 1.0, -1.0, -2.0)
    }
  }
}

object MathLibSpec extends MathLibSpec[test.YId] with test.YIdInstances

// vim: set ts=4 sw=4 et:
