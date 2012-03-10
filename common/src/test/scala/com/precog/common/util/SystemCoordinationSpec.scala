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
package com.precog.common
package util

import org.specs2.execute._
import org.specs2.mutable._
import org.specs2.specification._

import org.I0Itec.zkclient.ZkClient

class ZookeeperSystemCoordinationSpec extends Specification {

  case class zookeeperClient(zkHosts: String = "127.0.0.1:2181") extends AroundOutside[ZkClient] {

    private var client: ZkClient = null

    def newClient(): Option[ZkClient] = {
      try {
        Some(new ZkClient(zkHosts, 1000))
      } catch {
        case ex => None
      } 
    }

    private val offline = new Pending("SKIP - ZOOKEEPER NOT AVAILABLE")

    def around[T <% Result](t: =>T): Result = {
      newClient().map{ zkc => 
        client = zkc
        val result: Result = t
        try {
          client.deleteRecursive("/test")
          client.close
        } catch {
          case ex =>
        }
        result
      }.getOrElse(offline)
    }
    
    def outside: ZkClient = client 
  }

  "zookeeper system coordination " should {
    "return new producer ids" in zookeeperClient() { client: ZkClient =>
      todo
    } 
    "correctly unregister producer ids" in zookeeperClient() { client: ZkClient =>
      todo
    }
    "save checkpoints" in zookeeperClient() { client: ZkClient =>
      todo
    }
    "restore checkpoints" in zookeeperClient() { client: ZkClient =>
      todo
    }
  }
}
