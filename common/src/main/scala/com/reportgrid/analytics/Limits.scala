package com.reportgrid.analytics

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.JodaSerializationImplicits._

import org.joda.time.DateTime
import scalaz.Scalaz._
import scalaz.Validation

/** Limits represent hard limits on the data aggregated by ReportGrid. They
 * are typically set at a customer-by-customer basis, depending on the plan
 * the customer signs up for. Higher limits imply more power in aggregation
 * but more resource utilization on the backend.
 */
case class Limits(order: Int, limit: Int, depth: Int, tags: Int = 1, lossless: Boolean = true, rollup: Int = 0) {
  def limitTo(that: Limits) = Limits(
    order = this.order.min(that.order),
    limit = this.limit.min(that.limit),
    depth = this.depth.min(that.depth),
    tags  = this.tags.min(that.tags),
    rollup = this.rollup.min(that.rollup)
  )
}

trait LimitSerialization {
  final implicit val LimitsDecomposer = new Decomposer[Limits] {
    def decompose(limits: Limits): JValue = JObject(
      JField("order", limits.order.serialize) ::
      JField("limit", limits.limit.serialize) ::
      JField("depth", limits.depth.serialize) ::
      JField("tags",  limits.tags.serialize) ::
      JField("lossless",  limits.lossless.serialize) ::
      JField("rollup",  limits.rollup.serialize) ::
      Nil
    )
  }

  final implicit val LimitsExtractor = new Extractor[Limits] with ValidatedExtraction[Limits] {
    override def validated(jvalue: JValue) = {
      (
        (jvalue \ "order").validated[Int] |@|
        (jvalue \ "limit").validated[Int] |@|
        (jvalue \ "depth").validated[Int] |@|
        (jvalue \ "tags").validated[Int] 
      ) {
        Limits(_, _, _, _, (jvalue \ "lossless").validated[Boolean] | true, (jvalue \ "rollup").validated[Int] | 0)
      }
    }
  }

  def limitsExtractor(default: Limits) = new Extractor[Limits] {
    override def extract(jvalue: JValue) = Limits(
      (jvalue \ "order").validated[Int] | default.order,
      (jvalue \ "limit").validated[Int] | default.limit,
      (jvalue \ "depth").validated[Int] | default.depth,
      (jvalue \ "tags").validated[Int]  | default.tags,
      (jvalue \ "lossless").validated[Boolean]  | default.lossless,
      (jvalue \ "rollup").validated[Int] | default.rollup
    ) 
  }
}

object Limits extends LimitSerialization {
  val None = Limits(100, 100, 100, 100, true, 100)
}
