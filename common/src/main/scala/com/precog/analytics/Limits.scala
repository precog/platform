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
package com.precog.analytics

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
