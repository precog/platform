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
  accountsPath: String, authPort: Int, authPath: String, ingestPort: Int,
  ingestPath: String, jobsPort: Int, jobsPath: String, shardPort: Int,
  shardPath: String, secure: Boolean)

object Settings {
  private val HostRegex = """^host (.+)$""".r
  private val IdRegex = """^id (.+)$""".r
  private val TokenRegex = """^token ([-A-F0-9]+)$""".r
  private val AccountsPort = """^accounts (\d+)$""".r
  private val AccountsPath = """^accounts\-path (.+)$""".r
  private val AuthPort = """^auth (\d+)$""".r
  private val AuthPath = """^auth\-path (.+)$""".r
  private val IngestPort = """^ingest (\d+)$""".r
  private val IngestPath = """^ingest\-path (.+)$""".r
  private val JobsPort = """^jobs (\d+)$""".r
  private val JobsPath = """^jobs\-path (.+)$""".r
  private val ShardPort = """^bifrost (\d+)$""".r
  private val ShardPath = """^bifrost\-path (.+)$""".r
  private val SecureRegex = """^secure (true|false)$""".r

  def fromFile(f: File): Settings = {
    if (!f.canRead) sys.error("Can't read %s. Is bifrost running?" format f)

    case class PartialSettings(
        host: Option[String] = None,
        id: Option[String] = None,
        token: Option[String] = None,
        accountsPort: Option[Int] = None,
        accountsPath: Option[String] = None,
        authPort: Option[Int] = None,
        authPath: Option[String] = None,
        ingestPort: Option[Int] = None,
        ingestPath: Option[String] = None,
        jobsPort: Option[Int] = None,
        jobsPath: Option[String] = None,
        shardPort: Option[Int] = None,
        shardPath: Option[String] = None,
        secure: Option[Boolean] = None) {

      def missing: List[String] = {
        def q(o: Option[_], s: String): List[String] =
          if (o.isDefined) Nil else s :: Nil

        q(host, "host") ++ q(id, "id") ++ q(token, "token") ++
        q(accountsPort, "accountsPort") ++ q(accountsPath, "accountsPath") ++
        q(authPort, "authPort") ++ q(authPath, "authPath") ++ 
        q(ingestPort, "ingestPort") ++ q(ingestPath, "ingestPath") ++ 
        q(jobsPort, "jobsPort") ++ q(jobsPath, "jobsPath") ++ 
        q(shardPort, "shardPort") ++ q(shardPath, "shardPath") ++ q(secure, "secure")
      }

      def settings: Option[Settings] = for {
        h <- host
        i <- id
        t <- token
        ac <- accountsPort
        acp <- accountsPath
        au <- authPort
        aup <- authPath
        in <- ingestPort
        inp <- ingestPath
        j <- jobsPort
        jp <- jobsPath
        sh <- shardPort
        shp <- shardPath
        sec <- secure
      } yield {
        Settings(h, i, t, ac, acp, au, aup, in, inp, j, jp, sh, shp, sec)
      }
    }

    val lines = io.Source.fromFile(f).getLines
    
    val defaults = PartialSettings(
      host = Some("localhost"),
      accountsPath = Some("accounts"),
      authPath = Some("apikeys"),
      ingestPath = Some("ingest"),
      shardPath = Some("meta"),
      jobsPath = Some("jobs/v1"),
      secure = Some(false))
    
    val ps = lines.foldLeft(defaults) { (ps, s) =>
      val ps2 = s match {
        case HostRegex(s) => ps.copy(host = Some(s))
        case IdRegex(s) => ps.copy(id = Some(s))
        case TokenRegex(s) => ps.copy(token = Some(s))
        case AccountsPort(n) => ps.copy(accountsPort = Some(n.toInt))
        case AccountsPath(p) => ps.copy(accountsPath = Some(p))
        case AuthPort(n) => ps.copy(authPort = Some(n.toInt))
        case AuthPath(p) => ps.copy(authPath = Some(p))
        case _ => ps
      }
      
      // split to avoid a bug in the pattern matcher
      s match {
        case IngestPort(n) => ps2.copy(ingestPort = Some(n.toInt))
        case IngestPath(p) => ps2.copy(ingestPath = Some(p))
        case JobsPort(n) => ps2.copy(jobsPort = Some(n.toInt))
        case JobsPath(p) => ps2.copy(jobsPath = Some(p))
        case ShardPort(n) => ps2.copy(shardPort = Some(n.toInt))
        case ShardPath(p) => ps2.copy(shardPath = Some(p))
        case SecureRegex(s) => ps2.copy(secure = Some(s.toBoolean))
        case _ => ps2
      }
    }
    ps.settings.getOrElse {
      sys.error("missing settings in %s:\n  %s" format (f, ps.missing.mkString("\n  ")))
    }
  }
}
