package com.precog.shard.util

import com.precog.common.util._

import blueeyes.json.Printer
import blueeyes.json.JsonAST._

object QuickGen extends App {
  
  import AdSamples._

  val datasets = Map( 
    ("campaigns"      -> adCampaignSample),
    ("organizations"  -> adOrganizationSample),
    ("clicks"         -> interactionSample),
    ("impressions"    -> interactionSample2),
    ("users"          -> usersSample),
    ("orders"         -> ordersSample),
    ("payments"       -> paymentsSample),
    ("pageViews"      -> pageViewsSample),
    ("customers"      -> customersSample)
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
