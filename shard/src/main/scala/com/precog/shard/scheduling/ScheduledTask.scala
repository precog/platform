package com.precog.shard
package scheduling

import com.precog.common.Path
import com.precog.common.security.{APIKey, Authorities}
import com.precog.daze.QueryOptions

import java.util.UUID

import org.quartz.CronExpression

case class ScheduledTask(id: UUID, schedule: CronExpression, apiKey: APIKey, authorities: Authorities, prefix: Path, source: Path, sink: Path, opts: QueryOptions) {
  def taskName = "Scheduled %s -> %s".format(source, sink)
}
