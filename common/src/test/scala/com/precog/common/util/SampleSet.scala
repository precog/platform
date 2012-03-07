package com.precog.common.util

import org.joda.time._
import org.joda.time.format._
import org.joda.time.DateTimeZone

import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import scalaz.syntax.std.booleanV._

trait SampleSet[T] {
  // the number of samples to return in the queriablesamples list
  def queriableSampleSize: Int

  // Samples that have been injected and thus can be used to construct
  // queries that will return results. Returns None until the requisite
  // number of queriable samples has been reached.
  def queriableSamples: Option[Vector[T]]

  def next: (T, SampleSet[T])
}

object AdSamples {
  val genders = List("male", "female")
  val employees = List("0-25","25-100","100-250","250-1000","1000-5000","5000-10000","10000+")
  val revenue = List("<500K", "500K-5M", "5-50M", "50-250M", "250-500M", "500M+")
  val category = List("electronics", "fashion", "travel", "media", "sundries", "magical")
  val ageTuples = List((0,17),(18,24),(25,36),(37,48),(49,60),(61,75),(76,130))
  val ageRangeStrings = ageTuples map { case (l, h) => "%d-%d".format(l,h) }
  val ageRangeArrays = ageTuples map { case (l, h) => JArray(List(JInt(l), JInt(h))) }
  val platforms = List("android", "iphone", "web", "blackberry", "other")
  val campaigns = for (i <- 0 to 30) yield "c" + i
  val pageId = for (i <- 0 to 4) yield "page-" + i
  val userId = for (i <- 1000 to 1020) yield "user-" + i
  val eventNames = List("impression", "click", "conversion")
  val timeISO8601 = List("2010-11-04T15:38:12.782+03:00", "2010-04-22T06:22:38.039+06:30", "2009-05-30T12:31:42.462-09:00", "2009-02-11T22:12:18.493-02:00", "2008-09-19T06:28:31.325+10:00")
  val timeZone = List("-12:00", "-11:00", "-10:00", "-09:00", "-08:00", "-07:00", "-06:00", "-05:00", "-04:00", "-03:00", "-02:00", "-01:00", "+00:00", "+01:00", "+02:00", "+03:00", "+04:00", "+05:00", "+06:00", "+07:00", "+08:00", "+09:00", "+10:00", "+11:00", "+12:00", "+13:00", "+14:00")
  
  val states = 
    List("AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", 
    "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MT",  
    "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR",  
    "MD", "MA", "MI", "MN", "MS", "MO", "PA", "RI", "SC", "SD", "TN",  
    "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY") 

  val shippingRates = List(5.95,6.95,10.95,24.95)
  val handlingCharges = List(5.00,7.00,10.00,0)

  val departments = List("sales", "marketing", "operations", "engineering", "manufacturing", "research")
  def gaussianIndex(size: Int): Gen[Int] = {
    Gen( p => {
      def sample: Double = {
        val testIndex = (p.rng.nextGaussian * (size / 5)) + (size / 2)
        if (testIndex < 0 || testIndex >= size) sample
        else testIndex
      }

      Some(sample.toInt)
    })
  }

  def exponentialIndex(size: Int): Gen[Int] = {
    Gen( p => {
      import scala.math._
      Some(round(exp(-p.rng.nextDouble * 8) * size).toInt.min(size - 1).max(0))
    })
  }

  def defaultSample = adCampaignSample

  def adCampaignSample = for {
    gender <- oneOf(genders)
    plat <- exponentialIndex(platforms.size).map{ platforms(_) }
    camp <- gaussianIndex(campaigns.size).map{ campaigns(_) }
    cpm <- chooseNum(1, 100)
    ageRange <- gaussianIndex(ageRangeArrays.size).map{ ageRangeArrays(_) }
  } yield {
    JObject(
      JField("gender", gender) ::
      JField("platform", plat) ::
      JField("campaign", camp) ::
      JField("cpm", cpm) ::
      JField("ageRange", ageRange) :: Nil
    )
  }

  def adOrganizationSample = for {
    emps <- oneOf(employees)
    rev <- oneOf(revenue)
    cat <- oneOf(category)
    camp <- gaussianIndex(campaigns.size).map{ campaigns(_) } 
  } yield {
    JObject(
      JField("employees", emps) ::
      JField("revenue", rev) ::
      JField("category", cat) ::
      JField("campaign", camp) :: Nil
    )
  }

  def interactionSample = for {
    time <- earlierTimeFrame
    tz <- oneOf(timeZone)
    ts <- ISO8601(time, tz)
    pid <- oneOf(pageId)
    uid <- oneOf(userId)
  } yield {
    JObject(
      JField("time", time) ::
      JField("timeZone", tz) ::
      JField("timeString", toISO8601(time, tz)) ::
      JField("pageId", pid) :: 
      JField("userId", uid) :: Nil
    )
  }

  def interactionSample2 = for {
    time <- laterTimeFrame
    tz <- oneOf(timeZone)
    pid <- oneOf(pageId)
    uid <- oneOf(userId)
  } yield {
    JObject(
      JField("time", time) ::
      JField("timeZone", tz) ::
      JField("pageId", pid) :: 
      JField("userId", uid) :: Nil
    )
  }

  def eventsSample = for {
    time <- ISO8601(laterTimeFrame, oneOf(timeZone))
    platform <- oneOf(platforms)
    eventName <- oneOf(eventNames)
  } yield {
    JObject(
      JField("time", time) ::
      JField("platform", platform) :: 
      JField("eventName", eventName) :: Nil
    )
  }
  
  def usersSample = for {
    age <- chooseNum(18,100)
    income <- chooseNum(10,250).map{ _ * 1000 }
    state <- oneOf(states)
  } yield {
    JObject(
      JField("age", age) :: 
      JField("income", income) ::  
      JField("location", JObject(JField("state", state) :: Nil)) :: Nil 
    )
  }

  def ordersSample = for {
    userId <- chooseNum(12345, 12545)
    taxRate <- chooseNum(70,110).map { _.toDouble / 100 }
    subTotal <- chooseNum(123, 11145).map { _.toDouble / 100 }
    shipping <- oneOf(shippingRates)
    handling <- oneOf(handlingCharges)
    val total = subTotal * taxRate + shipping + handling
  } yield {
    JObject(
      JField("userId", userId) ::
      JField("total", total) ::
      JField("taxRate", taxRate) ::
      JField("subTotal", subTotal) :: 
      JField("shipping", shipping) :: 
      JField("handling", handling) :: Nil
    )
  }

  def recipientsSample = listOfN(2, oneOf(departments)).map { list => 
    JArray( list.map { JString(_) } )
  }

  def paymentsSample = for {
    date <- earlierTimeFrame
    recipients <- recipientsSample 
    amount <- chooseNum(500, 5000).map( _.toDouble / 100)
  } yield {
    JObject(
      JField("date", date ) :: 
      JField("recipients", recipients) :: 
      JField("amount", amount) :: Nil
    )
  }

  def pageViewsSample = for {
    duration <- chooseNum(1,300)
    userId <- chooseNum(12345, 12360)
  } yield {
    JObject(
      JField("duration", duration) :: 
      JField("userId", userId) :: Nil
    )
  }

  def customersSample = for {
    userId <- chooseNum(12345, 12545)
    income <- chooseNum(10,250).map( _ * 1000)
  } yield {
    JObject(
      JField("userId", userId) ::
      JField("income", income) :: Nil
    )
  }
  
  val millisPerDay: Long = 24L * 60 * 60 * 1000

  def earlierTimeFrame = chooseNum(System.currentTimeMillis - (20 * millisPerDay), System.currentTimeMillis - (10 * millisPerDay))
  def laterTimeFrame = chooseNum(System.currentTimeMillis - (10 * millisPerDay), System.currentTimeMillis)

  def ISO8601(timeGen: Gen[Long], timeZoneGen: Gen[String]) = for {
    time <- timeGen
    tz <- timeZoneGen
  } yield {
    val format = ISODateTimeFormat.dateTime()
    val timeZone = DateTimeZone.forID(tz.toString)
    val dateTime = new DateTime(time, timeZone)
    format.print(dateTime)
  }

  def toISO8601(time: Long, tz: String): String = {
    val format = ISODateTimeFormat.dateTime()
    val timeZone = DateTimeZone.forID(tz.toString)
    val dateTime = new DateTime(time.toLong, timeZone)
    format.print(dateTime)
  }

}

case class DistributedSampleSet[T](val queriableSampleSize: Int, sampler: Gen[T], private val recordedSamples: Vector[T] = Vector()) extends SampleSet[T] { self =>
  def queriableSamples = (recordedSamples.size >= queriableSampleSize).option(recordedSamples)

  import AdSamples._
  def next = {
    val sample = sampler.sample.get

    // dumb sample accumulation, just takes the first n samples recorded
    (sample, if (recordedSamples.size >= queriableSampleSize) this else this.copy(recordedSamples = recordedSamples :+ sample))
  }
}

object DistributedSampleSet {
  def sample[T](sampleSize: Int, queriableSamples: Int, sampler: Gen[T] = AdSamples.defaultSample): (Vector[T], Option[Vector[T]]) = {
    def pull[T](sampleSet: SampleSet[T], sampleData: Vector[T], counter: Int): (SampleSet[T], Vector[T]) = {
      if (counter < sampleSize) {
        val (event, nextSet) = sampleSet.next
        pull(nextSet, sampleData :+ event, counter + 1)
      } else {
        (sampleSet, sampleData)
      }
    }

    val (sampleSet, data) = pull(DistributedSampleSet(queriableSamples, sampler), Vector(), 0)
    (data, sampleSet.queriableSamples)
  }
}


// vim: set ts=4 sw=4 et:
