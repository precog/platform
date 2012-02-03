package com.precog.yggdrasil.util

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
  val ageRanges = List("0-17", "18-24", "25-36", "37-48", "49-60", "61-75", "76-130")
  val platforms = List("android", "iphone", "web", "blackberry", "other")
  val campaigns = for (i <- 0 to 30) yield "c" + i
  val eventNames = List("impression", "click", "conversion")

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
}

case class DistributedSampleSet(queriableSampleSize: Int, private val recordedSamples: Vector[JObject] = Vector()) extends SampleSet { self =>
  def queriableSamples = (recordedSamples.size >= queriableSampleSize).option(recordedSamples)

  import AdSamples._
  def next = {
    val sample = JObject(
      JField("gender", oneOf(genders).sample.get) ::
      JField("platform", platforms(exponentialIndex(platforms.size))) ::
      JField("campaign", campaigns(gaussianIndex(campaigns.size))) ::
      JField("cpm", chooseNum(1, 100).sample.get) ::
      JField("ageRange", ageRanges(gaussianIndex(ageRanges.size))) :: Nil
    )

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
