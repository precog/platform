package com.precog.common.util

import org.joda.time._
import org.joda.time.format._
import org.joda.time.DateTimeZone

import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import org.scalacheck.Gen._
import scalaz.syntax.std.booleanV._

trait SampleSet {
  // the number of samples to return in the queriablesamples list
  def queriableSampleSize: Int

  // Samples that have been injected and thus can be used to construct
  // queries that will return results. Returns None until the requisite
  // number of queriable samples has been reached.
  def queriableSamples: Option[Vector[JObject]]

  def next: (JObject, SampleSet)
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
  def gaussianIndex(size: Int): Int = {
    // multiplying by size / 5 means that 96% of the time, the sampled value will be within the range and no second try will be necessary
    val testIndex = (scala.util.Random.nextGaussian * (size / 5)) + (size / 2)
    if (testIndex < 0 || testIndex >= size) gaussianIndex(size)
    else testIndex.toInt
  }

  def exponentialIndex(size: Int): Int = {
    import scala.math._
    round(exp(-random * 8) * size).toInt.min(size - 1).max(0)
  }

  def defaultSample() = adCampaignSample

  def adCampaignSample() = JObject(
      JField("gender", oneOf(genders).sample.get) ::
      JField("platform", platforms(exponentialIndex(platforms.size))) ::
      JField("campaign", campaigns(gaussianIndex(campaigns.size))) ::
      JField("cpm", chooseNum(1, 100).sample.get) ::
      JField("ageRange", ageRangeArrays(gaussianIndex(ageRangeArrays.size))) :: Nil
    )

  def adOrganizationSample() = JObject(
    JField("employees", oneOf(employees).sample.get) ::
    JField("revenue", oneOf(revenue).sample.get) ::
    JField("category", oneOf(category).sample.get) ::
    JField("campaign", campaigns(gaussianIndex(campaigns.size))) :: Nil
  )

  def interactionSample() = {
    val time = earlierTimeFrame.sample.get
    val timezone = oneOf(timeZone).sample.get
    JObject(
      JField("time", time) ::
      JField("timeZone", timezone) ::
      JField("timeString", toISO8601(time, timezone)) ::
      JField("pageId", oneOf(pageId).sample.get) :: 
      JField("userId", oneOf(userId).sample.get) :: Nil
    )
  }

  def interactionSample2() = JObject(
    JField("time", laterTimeFrame.sample.get) ::
    JField("timeZone", oneOf(timeZone).sample.get) ::
    JField("pageId", oneOf(pageId).sample.get) :: 
    JField("userId", oneOf(userId).sample.get) :: Nil
  )

  def eventsSample() = JObject(
    JField("time", toISO8601(laterTimeFrame.sample.get, oneOf(timeZone).sample.get)) ::
    JField("platform", oneOf(platforms).sample.get) :: 
    JField("eventName", oneOf(eventNames).sample.get) :: Nil
  )
  
  def usersSample() = JObject(
    JField("age", chooseNum(18,100).sample.get  ) :: 
    JField("income", chooseNum(10,250).sample.get * 1000 ) ::  
    JField("location", JObject(JField("state", oneOf(states).sample.get) :: Nil)) :: Nil 
  )

  def ordersSample() = {
    val taxRate = chooseNum(70,110).sample.get.toDouble / 1000 
    val subTotal = chooseNum(123,11145).sample.get.toDouble / 100
    val shipping = oneOf(shippingRates).sample.get
    val handling = oneOf(handlingCharges).sample.get
    val total = subTotal * taxRate + shipping + handling
    JObject(
      JField("userId", chooseNum(12345,12545).sample.get) ::
      JField("total", total) ::
      JField("taxRate", taxRate) ::
      JField("subTotal", subTotal) :: 
      JField("shipping", shipping) :: 
      JField("handling", handling) :: Nil
    )
  }

  def recipients() = JArray(
    listOfN(2, oneOf(departments)).sample.get.map(JString(_))
  )

  def paymentsSample() = JObject(
    JField("date", earlierTimeFrame.sample.get ) :: 
    JField("recipients", recipients) :: 
    JField("amount", chooseNum(500,50000).sample.get.toDouble / 100) :: Nil
  )

  def pageViewsSample() = JObject(
    JField("duration", chooseNum(1,300).sample.get) :: 
    JField("userId", chooseNum(12345,12360).sample.get) :: Nil
  )

  def customersSample() = JObject(
    JField("userId", chooseNum(12345,12545).sample.get) ::
    JField("income", chooseNum(10,250).sample.get * 1000) :: Nil
  )
  
  val millisPerDay: Long = 24L * 60 * 60 * 1000

  def earlierTimeFrame = chooseNum(System.currentTimeMillis - (20 * millisPerDay), System.currentTimeMillis - (10 * millisPerDay))
  def laterTimeFrame = chooseNum(System.currentTimeMillis - (10 * millisPerDay), System.currentTimeMillis)

  def toISO8601(time: Long, tz: String): String = {
    val format = ISODateTimeFormat.dateTime()
    val timeZone = DateTimeZone.forID(tz.toString)
    val dateTime = new DateTime(time.toLong, timeZone)
    format.print(dateTime)
  }

}

case class DistributedSampleSet(val queriableSampleSize: Int, private val recordedSamples: Vector[JObject] = Vector(), sampler: () => JObject = AdSamples.defaultSample _) extends SampleSet { self =>
  def queriableSamples = (recordedSamples.size >= queriableSampleSize).option(recordedSamples)

  import AdSamples._
  def next = {
    val sample = sampler()

    // dumb sample accumulation, just takes the first n samples recorded
    (sample, if (recordedSamples.size >= queriableSampleSize) this else this.copy(recordedSamples = recordedSamples :+ sample))
  }
}

object DistributedSampleSet {
  def sample(sampleSize: Int, queriableSamples: Int): (Vector[JObject], Option[Vector[JObject]]) = {
    def pull(sampleSet: SampleSet, sampleData: Vector[JObject], counter: Int): (SampleSet, Vector[JObject]) = {
      if (counter < sampleSize) {
        val (event, nextSet) = sampleSet.next
        pull(nextSet, sampleData :+ event, counter + 1)
      } else {
        (sampleSet, sampleData)
      }
    }

    val (sampleSet, data) = pull(DistributedSampleSet(queriableSamples), Vector(), 0)
    (data, sampleSet.queriableSamples)
  }
}


// vim: set ts=4 sw=4 et:
