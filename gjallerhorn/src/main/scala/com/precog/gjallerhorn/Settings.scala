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
package com.precog.gjallerhorn

import java.io._

case class Settings(host: String, id: String, token: String, accountsPort: Int,
  authPort: Int, ingestPort: Int, jobsPort: Int, shardPort: Int)

object Settings {
  private val IdRegex = """^id (.+)$""".r
  private val TokenRegex = """^token ([-A-F0-9]+)$""".r
  private val AccountsPort = """^accounts (\d+)$""".r
  private val AuthPort = """^auth (\d+)$""".r
  private val IngestPort = """^ingest (\d+)$""".r
  private val JobsPort = """^jobs (\d+)$""".r
  private val ShardPort = """^shard (\d+)$""".r

  def fromFile(f: File): Settings = {
    if (!f.canRead) sys.error("Can't read %s. Is shard running?" format f)

    case class PartialSettings(
      host: Option[String] = None,
      id: Option[String] = None, token: Option[String] = None,
      accountsPort: Option[Int] = None, authPort: Option[Int] = None,
      ingestPort: Option[Int] = None, jobsPort: Option[Int] = None,
      shardPort: Option[Int] = None) {

      def missing: List[String] = {
        def q(o: Option[_], s: String): List[String] =
          if (o.isDefined) Nil else s :: Nil

        q(host, "host") ++ q(id, "id") ++ q(token, "token") ++
        q(accountsPort, "accountsPort") ++ q(authPort, "authPort") ++
        q(ingestPort, "ingestPort") ++ q(jobsPort, "jobsPort") ++
        q(shardPort, "shardPort")
      }

      def settings: Option[Settings] = for {
        h <- host
        i <- id
        t <- token
        ac <- accountsPort
        au <- authPort
        in <- ingestPort
        j <- jobsPort
        sh <- shardPort
      } yield {
        Settings(h, i, t, ac, au, in, j, sh)
      }
    }

    val lines = io.Source.fromFile(f).getLines
    val ps = lines.foldLeft(PartialSettings(host = Some("localhost"))) { (ps, s) =>
      s match {
        case IdRegex(s) => ps.copy(id = Some(s))
        case TokenRegex(s) => ps.copy(token = Some(s))
        case AccountsPort(n) => ps.copy(accountsPort = Some(n.toInt))
        case AuthPort(n) => ps.copy(authPort = Some(n.toInt))
        case IngestPort(n) => ps.copy(ingestPort = Some(n.toInt))
        case JobsPort(n) => ps.copy(jobsPort = Some(n.toInt))
        case ShardPort(n) => ps.copy(shardPort = Some(n.toInt))
        case _ => ps
      }
    }
    ps.settings.getOrElse {
      sys.error("missing settings in %s:\n  %s" format (f, ps.missing.mkString("\n  ")))
    }
  }
}
