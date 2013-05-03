package com.precog.daze

import org.specs2.mutable._

import com.precog.common._
import com.precog.yggdrasil._
import com.precog.common.Path
import scalaz._
import scalaz.std.list._

import com.precog.util.IdGen

import org.joda.time._
import org.joda.time.format._

trait TimeDifferenceSpecs[M[+_]] extends Specification
    with EvaluatorTestSupport[M]
    with LongIdMemoryDatasetConsumer[M] { self =>
      
  import Function._
  
  import dag._
  import instructions._
  import library._

  val testAPIKey = "testAPIKey"

  val line = Line(1, 1, "")

  def testEval(graph: DepGraph): Set[SEvent] = {
    consumeEval(testAPIKey, graph, Path.Root) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  "time difference functions (homogeneous case)" should {
    "compute difference of years" in {
      val input = Join(BuiltInFunction2Op(YearsBetween), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line),
        Const(CString("2010-09-23T18:33:22.520-10:00"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(-2, -1, 0)
    }
    "compute difference of months" in {
      val input = Join(BuiltInFunction2Op(MonthsBetween), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line),
        Const(CString("2010-09-23T18:33:22.520-10:00"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(-16, -4, -27, 4, -11)
    }
    "compute difference of weeks" in {
      val input = Join(BuiltInFunction2Op(WeeksBetween), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line),
        Const(CString("2010-09-23T18:33:22.520-10:00"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(-49, -118, -72, -21, 21)
    }
    "compute difference of days" in {
      val input = Join(BuiltInFunction2Op(DaysBetween), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line),
        Const(CString("2010-09-23T18:33:22.520-10:00"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(-505, -347, 148, -826, -150)
    }
    "compute difference of hours" in {
      val input = Join(BuiltInFunction2Op(HoursBetween), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line),
        Const(CString("2010-09-23T18:33:22.520-10:00"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }
      
      result2 must contain(-12131, -3606, -19836, -8340, 3554)
    }
    "compute difference of minutes" in {
      val input = Join(BuiltInFunction2Op(MinutesBetween), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line),
        Const(CString("2010-09-23T18:33:22.520-10:00"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }
      
      result2 must contain(-727898, 213295, -216396, -500411, -1190164)
    }
    "compute difference of seconds" in {
      val input = Join(BuiltInFunction2Op(SecondsBetween), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line),
        Const(CString("2010-09-23T18:33:22.520-10:00"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }
      
      result2 must contain(-30024690, -43673890, -12983796, -71409896, 12797729)
    }
    "compute difference of ms" in {
      val input = Join(BuiltInFunction2Op(MillisBetween), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line),
        Const(CString("2010-09-23T18:33:22.520-10:00"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }
      
      result2 must contain(12797729921L, -12983796645L, -30024690328L, -43673890874L, -71409896910L)
    }
  }

  "time difference functions (heterogeneous case)" should {
    "compute difference of years" in {
      val input = Join(BuiltInFunction2Op(YearsBetween), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601"))(line))(line),
        Const(CString("2010-09-23T18:33:22.520-10:00"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(-2, -1, 0)
    }
    "compute difference of months" in {
      val input = Join(BuiltInFunction2Op(MonthsBetween), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601"))(line))(line),
        Const(CString("2010-09-23T18:33:22.520-10:00"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(-16, -4, -27, 4, -11)
    }
    "compute difference of weeks" in {
      val input = Join(BuiltInFunction2Op(WeeksBetween), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601"))(line))(line),
        Const(CString("2010-09-23T18:33:22.520-10:00"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(-49, -118, -72, -21, 21)
    }
    "compute difference of days" in {
      val input = Join(BuiltInFunction2Op(DaysBetween), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601"))(line))(line),
        Const(CString("2010-09-23T18:33:22.520-10:00"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(-505, -347, 148, -826, -150)
    }
    "compute difference of hours" in {
      val input = Join(BuiltInFunction2Op(HoursBetween), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601"))(line))(line),
        Const(CString("2010-09-23T18:33:22.520-10:00"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }
      
      result2 must contain(-12131, -3606, -19836, -8340, 3554)
    }
    "compute difference of minutes" in {
      val input = Join(BuiltInFunction2Op(MinutesBetween), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601"))(line))(line),
        Const(CString("2010-09-23T18:33:22.520-10:00"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }
      
      result2 must contain(-727898, 213295, -216396, -500411, -1190164)
    }
    "compute difference of seconds" in {
      val input = Join(BuiltInFunction2Op(SecondsBetween), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601"))(line))(line),
        Const(CString("2010-09-23T18:33:22.520-10:00"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }
      
      result2 must contain(-30024690, -43673890, -12983796, -71409896, 12797729)
    }
    "compute difference of ms" in {
      val input = Join(BuiltInFunction2Op(MillisBetween), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601"))(line))(line),
        Const(CString("2010-09-23T18:33:22.520-10:00"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }
      
      result2 must contain(12797729921L, -12983796645L, -30024690328L, -43673890874L, -71409896910L)
    }
  }

  "time difference functions (homogeneous case across slices)" should {
    "compute difference of years" in {
      val input = Join(BuiltInFunction2Op(YearsBetween), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601AcrossSlices"))(line))(line),
        Const(CString("2010-09-23T18:33:22.520-10:00"))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(0, 1, 2, 3, -1, -2)
    }
    "compute difference of months" in {
      val input = Join(BuiltInFunction2Op(MonthsBetween), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601AcrossSlices"))(line))(line),
        Const(CString("2010-09-23T18:33:22.520-10:00"))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(-27, 10, -22, 14, 28, -4, 13, 41, 32, -5, -10, 7, 16, 43, -13, 30, -24, -17)
    }
    "compute difference of weeks" in {
      val input = Join(BuiltInFunction2Op(WeeksBetween), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601AcrossSlices"))(line))(line),
        Const(CString("2010-09-23T18:33:22.520-10:00"))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(-98, -96, -118, -76, 189, 121, 132, 70, -75, -23, 141, -19, 32, 59, -20, -106, 182, 72, -56, 47, -45, 62)
    }
    "compute difference of days" in {
      val input = Join(BuiltInFunction2Op(DaysBetween), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601AcrossSlices"))(line))(line),
        Const(CString("2010-09-23T18:33:22.520-10:00"))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(1327, 930, -826, -536, -675, 1279, 853, -139, -691, -163, 509, 434, 494, 330, -747, 226, -527, -321, -398, 415, 987, -144)
    }
    "compute difference of hours" in {
      val input = Join(BuiltInFunction2Op(HoursBetween), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601AcrossSlices"))(line))(line),
        Const(CString("2010-09-23T18:33:22.520-10:00"))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }

      result2 must contain(-3458, 31864, 11859, -12655, -9552, -3347, -16600, -12887, 12233, 22340, -16211, -17950, -19836, 10417, 23694, -3923, 20483, 9983, -7713, 30710, 7925, 5445)
    }
    "compute difference of minutes" in {
      val input = Join(BuiltInFunction2Op(MinutesBetween), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601AcrossSlices"))(line))(line),
        Const(CString("2010-09-23T18:33:22.520-10:00"))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }

      result2 must contain(-573157, 1911875, 1228981, 599021, -1077003, 1421696, -1190164, 1340430, -235403, -773234, 1842644, -462836, 625023, -200840, -972705, 326713, 475549, 711599, -996031, 733998, -759345, -207496)
    }
    "compute difference of seconds" in {
      val input = Join(BuiltInFunction2Op(SecondsBetween), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601AcrossSlices"))(line))(line),
        Const(CString("2010-09-23T18:33:22.520-10:00"))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }

      result2 must contain(-12050411, 42695984, 44039920, 114712508, -46394099, -64620189, -71409896, 28532980, 35941265, -27770192, -12449791, 80425853, 110558640, 19602785, 73738905, -34389461, -45560737, 37501386, -59761902, -58362317, -14124214, 85301793)
    }
    "compute difference of ms" in {
      val input = Join(BuiltInFunction2Op(MillisBetween), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601AcrossSlices"))(line))(line),
        Const(CString("2010-09-23T18:33:22.520-10:00"))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }

      result2 must contain(114712508479L, -71409896910L, -45560737520L, -34389461903L, 35941265077L, 110558640261L, -64620189172L, 44039920965L, 28532980863L, 85301793775L, -46394099354L, -12449791417L, 19602785480L, 80425853610L, 37501386405L, 42695984162L, -59761902164L, -27770192599L, 73738905032L, -14124214357L, -12050411758L, -58362317732L)
    }
  }

  "time difference functions (heterogeneous case across slices)" should {
    "compute difference of years" in {
      val input = Join(BuiltInFunction2Op(YearsBetween), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601AcrossSlices"))(line))(line),
        Const(CString("2010-09-23T18:33:22.520-10:00"))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(0, 1, 2, 3, -1)
    }
    "compute difference of months" in {
      val input = Join(BuiltInFunction2Op(MonthsBetween), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601AcrossSlices"))(line))(line),
        Const(CString("2010-09-23T18:33:22.520-10:00"))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(-12, 38, -19, 13, 22, 27, -1, -9, 26)
    }
    "compute difference of weeks" in {
      val input = Join(BuiltInFunction2Op(WeeksBetween), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601AcrossSlices"))(line))(line),
        Const(CString("2010-09-23T18:33:22.520-10:00"))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(-54, -39, -8, 57, 121, 116, -4, 166, -84, 99)
    }
    "compute difference of days" in {
      val input = Join(BuiltInFunction2Op(DaysBetween), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601AcrossSlices"))(line))(line),
        Const(CString("2010-09-23T18:33:22.520-10:00"))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(-274, 849, -58, -588, 403, -384, 1167, -30, 699, 813)
    }
    "compute difference of hours" in {
      val input = Join(BuiltInFunction2Op(HoursBetween), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601AcrossSlices"))(line))(line),
        Const(CString("2010-09-23T18:33:22.520-10:00"))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }

      result2 must contain(9672, -737, -14130, -9219, 20388, -1405, 19517, 16795, -6582, 28025)
    }
    "compute difference of minutes" in {
      val input = Join(BuiltInFunction2Op(MinutesBetween), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601AcrossSlices"))(line))(line),
        Const(CString("2010-09-23T18:33:22.520-10:00"))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }

      result2 must contain(-394965, -847824, 1223285, -84316, 1007749, 580359, 1681543, 1171059, -44237, -553154)
    }
    "compute difference of seconds" in {
      val input = Join(BuiltInFunction2Op(SecondsBetween), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601AcrossSlices"))(line))(line),
        Const(CString("2010-09-23T18:33:22.520-10:00"))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }

      result2 must contain(-2654273, 100892632, -5059008, -23697928, 73397157, 70263579, -50869487, 60464942, -33189258, 34821554)
    }
    "compute difference of ms" in {
      val input = Join(BuiltInFunction2Op(MillisBetween), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601AcrossSlices"))(line))(line),
        Const(CString("2010-09-23T18:33:22.520-10:00"))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }

      result2 must contain(60464942676L, -50869487651L, 34821554007L, -2654273728L, 100892632209L, -5059008412L, -33189258109L, -23697928353L, 73397157662L, 70263579014L)
    }
  }
}

object TimeDifferenceSpecs extends TimeDifferenceSpecs[test.YId] with test.YIdInstances
