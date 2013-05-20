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
package com.precog.yggdrasil
package scheduling

import akka.dispatch.{ExecutionContext, Future, Promise}
import akka.util.Timeout

import blueeyes.bkka._
import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.persistence.mongo._
import blueeyes.persistence.mongo.dsl._

import com.precog.util.PrecogUnit

import com.weiglewilczek.slf4s.Logging

import java.util.UUID

import org.streum.configrity.Configuration

import scalaz._

case class MongoScheduleStorageSettings(
  tasks: String = "tasks",
  deletedTasks: String = "tasks_deleted",
  reports: String = "reports",
  timeout: Long = 10000
)

object MongoScheduleStorageSettings {
  val defaults = MongoScheduleStorageSettings()
}

object MongoScheduleStorage {
  def apply(config: Configuration)(implicit executor: ExecutionContext): (MongoScheduleStorage, Stoppable) = {
    val settings = MongoScheduleStorageSettings(
      config[String]("mongo.tasks", "tasks"),
      config[String]("mongo.tasks_deleted", "tasks_deleted"),
      config[String]("mongo.reports", "reports"),
      config[Long]("mongo.query_timeout", 10000)
    )

    val mongo = RealMongo(config.detach("mongo"))

    val database = mongo.database(config[String]("mongo.database", "schedules_v1"))

    val storage = new MongoScheduleStorage(mongo, database, settings)

    val dbStop = Stoppable.fromFuture(database.disconnect.fallbackTo(Future(())) flatMap { _ => mongo.close })

    (storage, dbStop)
  }
}

class MongoScheduleStorage private[MongoScheduleStorage] (mongo: Mongo, database: Database, settings: MongoScheduleStorageSettings)(implicit executor: ExecutionContext) extends ScheduleStorage[Future] with Logging {
  private implicit val M = new FutureMonad(executor)

  private implicit val timeout = new Timeout(settings.timeout)

  // ensure indexing on the collections
  database(ensureUniqueIndex("task_index").on(".id").in(settings.tasks))
  database(ensureIndex("report_index").on(".id").in(settings.reports))

  def addTask(task: ScheduledTask) = insertTask(-\/(task), settings.tasks) map { _ map { _ => task } }

  private def insertTask(task: ScheduledTask \/ JObject, collection: String) =
    database(insert(task.valueOr { st => st.serialize.asInstanceOf[JObject] }).into(collection)) map { _ => Success(PrecogUnit) }

  def deleteTask(id: UUID) =
    database(selectOne().from(settings.tasks).where(".id" === id.toString)) flatMap { ot =>
      ot map { task =>
        for {
          _ <- insertTask(\/-(task), settings.deletedTasks)
          _ <- database(remove.from(settings.tasks)where(".id" === id.toString))
        } yield Success(ot.map { _.deserialize[ScheduledTask] })
      } getOrElse {
        logger.warn("Could not locate task %s for deletion".format(id))
        Promise successful Success(None)
      }
    }

  def reportRun(report: ScheduledRunReport) =
    database(insert(report.serialize.asInstanceOf[JObject]).into(settings.reports)) map { _ => PrecogUnit }

  def statusFor(id: UUID, limit: Option[Int]) = {
    database(selectOne().from(settings.tasks).where(".id" === id.toString)) flatMap { taskOpt =>
      database(selectAll.from(settings.reports).where(".id" === id.toString)/* TODO: limit */) map { history =>
        taskOpt map { task =>
          (task.deserialize[ScheduledTask], history.toSeq map { _.deserialize[ScheduledRunReport] })
        }
      }
    }
  }

  def listTasks = database(selectAll.from(settings.tasks)) map { _.toSeq map { _.deserialize[ScheduledTask] } }
}
