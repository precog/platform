package com.precog.shard.util

import com.precog.common.util._

import blueeyes.json.Printer
import blueeyes.json.JsonAST._

object QuickGen extends App {
  val datasets = Map( 
    ("campaigns"      -> AdSamples.adCampaignSample _),
    ("organizations"  -> AdSamples.adOrganizationSample _),
    ("clicks"         -> AdSamples.interactionSample _),
    ("impressions"    -> AdSamples.interactionSample _),
    ("users"          -> NewSamples.usersSample _),
    ("orders"         -> NewSamples.ordersSample _),
    ("payments"       -> NewSamples.paymentsSample _),
    ("pageViews"      -> NewSamples.pageViewsSample _),
    ("customers"      -> NewSamples.customersSample _)
  )

  def usage() {
    println(
"""
Usage:

   command {dataset} {quantity}
"""
    )
  }

  def run(dataset: String, events: Int) {
    val sampler = datasets.get(dataset).getOrElse(sys.error("Unknown dataset name: " + dataset))
    val sampleSet = DistributedSampleSet(0, sampler = sampler)
    val sample = 0.until(events).map{ _ => sampleSet.next._1 }.toList
    println(Printer.pretty(Printer.render(JArray(sample))))
  }

  if(args.size < 2) {
    usage()
    System.exit(1)
  } else {
    run(args(0), args(1).toInt)
  }
}
