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
package com.precog.shard
package scheduling

import akka.dispatch.{ExecutionContext, Future, Promise}

import com.precog.util.PrecogUnit

import java.util.UUID

import scalaz.Success

class InMemoryScheduleStorage(implicit executor: ExecutionContext) extends ScheduleStorage[Future] {
  private[this] var tasks = Map.empty[UUID, ScheduledTask]
  private[this] var history = Map.empty[UUID, Seq[ScheduledRunReport]]

  def addTask(task: ScheduledTask) = Promise successful {
    tasks += (task.id -> task)
    Success(task)
  }

  def deleteTask(id: UUID) = Promise successful {
    val found = tasks.get(id)
    tasks -= id
    Success(found)
  }

  def reportRun(report: ScheduledRunReport) = Promise successful {
    history += (report.id -> (history.getOrElse(report.id, Seq.empty[ScheduledRunReport]) :+ report))
    PrecogUnit
  }

  def statusFor(id: UUID, limit: Option[Int]) = Promise successful {
    tasks.get(id) map { task =>
      val reports = history.getOrElse(id, Seq.empty[ScheduledRunReport])
      (task, limit map(reports.take) getOrElse reports)
    }
  }

  def listTasks = Promise successful {
    tasks.values.toSeq
  }
}
