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
  
  //val timeMillis = for (i <- 946684800000L to 1330257342000L) yield i
  //val timeZoneHours = for (i <- -11 to 12) yield i
  //val timeZoneMinutes = List(0, 30)
  

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

  def interactionSample() = JObject(
    JField("time", twoDayTimeFrame.sample.get) :: 
    JField("pageId", oneOf(pageId).sample.get) :: 
    JField("userId", oneOf(userId).sample.get) :: Nil
  )

  def interactionSampleISO8601() = JObject(
    JField("time", oneOf(timeISO8601).sample.get) :: 
    JField("pageId", oneOf(pageId).sample.get) :: 
    JField("userId", oneOf(userId).sample.get) :: Nil
  )
  
  val millisPerDay: Long = 24L * 60 * 60 * 1000

  def twoDayTimeFrame = chooseNum(System.currentTimeMillis - millisPerDay,
                            System.currentTimeMillis + millisPerDay)

  //val fmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ")
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
