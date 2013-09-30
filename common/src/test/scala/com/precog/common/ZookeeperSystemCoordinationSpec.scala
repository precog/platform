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

import com.precog.util._

import org.I0Itec.zkclient.ZkClient

import scala.collection.mutable.ListBuffer
import scalaz.{Success, Failure}

import org.specs2.execute._
import org.specs2.mutable._
import org.specs2.specification._

class ZookeeperSystemCoordinationSpec extends Specification {
  sequential

  "zookeeper system coordination " should {
    "register relay agent" in TestZookeeperClient() { factory: ClientFactory =>
      val client = factory()
      val sc = newSystemCoordination(client)

      val result = sc.registerRelayAgent("test_agent", 10000)

      result must beLike {
        case Success(EventRelayState(0, 1, IdSequenceBlock(0, 1, 10000))) => ok
      }
    }

    "renew relay agent" in TestZookeeperClient() { factory: ClientFactory =>
      val client = factory()
      val sc = newSystemCoordination(client)

      sc.registerRelayAgent("test_agent", 10000)
      val result = sc.renewEventRelayState("test_agent", 0L, 0, 10000)

      result must beLike {
        case Success(EventRelayState(0, 10001, IdSequenceBlock(0, 10001, 20000))) => ok
      }
    }

    "save and restore relay agent state" in TestZookeeperClient() { factory: ClientFactory =>
      val client1 = factory()
      val sc1 = newSystemCoordination(client1)

      sc1.registerRelayAgent("test_agent", 10000)
      val lastState = EventRelayState(123, 456, IdSequenceBlock(0, 1, 10000))
      val result1 = sc1.saveEventRelayState("test_agent", lastState)

      result1 must beLike {
        case Success(EventRelayState(123, 456, IdSequenceBlock(0, 1, 10000))) => ok
      }

      sc1.close

      val client2 = factory()
      val sc2 = newSystemCoordination(client2)

      val result2 = sc2.registerRelayAgent("test_agent", 10000)

      sc2.close

      result2 must beLike {
        case Success(EventRelayState(123, 456, IdSequenceBlock(0, 1, 10000))) => ok
      }
    }

    "restore relay agent state" in TestZookeeperClient() { factory: ClientFactory =>
      val client1 = factory()
      val sc1 = newSystemCoordination(client1)

      sc1.registerRelayAgent("test_agent", 10000)
      val lastState = EventRelayState(123, 456, IdSequenceBlock(0, 1, 10000))
      sc1.unregisterRelayAgent("test_agent", lastState)

      sc1.close()

      val client2 = factory()
      val sc2 = newSystemCoordination(client2)

      val result = sc2.registerRelayAgent("test_agent", 10000)

      result must beLike {
        case Success(EventRelayState(123, 456, IdSequenceBlock(0, 1, 10000))) => ok
      }
    }

    "relay agent registration retries in case of stale registration" in TestZookeeperClient() { factory: ClientFactory =>
      val client1 = factory()
      val sc1 = newSystemCoordination(client1)

      val result1 = sc1.registerRelayAgent("test_agent", 10000)

      result1 must beLike {
        case Success(EventRelayState(0, 1, IdSequenceBlock(0, 1, 10000))) => ok
      }

      sc1.close()

      val client2 = factory()
      val sc2 = newSystemCoordination(client2)

      val result2 = sc2.registerRelayAgent("test_agent", 10000)

      result2 must beLike {
        case Success(EventRelayState(0, 1, IdSequenceBlock(0, 1, 10000))) => ok
      }
    }

    "relay agent registration fails after reasonable attempt to detect stale registration" in TestZookeeperClient() { factory: ClientFactory =>
      val client1 = factory()
      val sc1 = newSystemCoordination(client1)

      val result1 = sc1.registerRelayAgent("test_agent", 10000)
      result1 must beLike {
        case Success(_) => ok
      }

      val client2 = factory()
      val sc2 = newSystemCoordination(client2)

      val result2 = sc2.registerRelayAgent("test_agent", 10000)
      result2 must beLike {
        case Failure(_) => ok
      }
    }

    "distinguish between normal and abnormal relay agent shutdown" in {
      todo
    }

    "handle sequenceid overflow by assigning new producer id" in {
      todo
    }

    "not load a missing checkpoint (checkpoints for new shards should be inserted manually via YggUtils)" in TestZookeeperClient() { factory: ClientFactory =>
      val client = factory()
      val sc = newSystemCoordination(client)

      val checkpoints = sc.loadYggCheckpoint("bifrost")

      checkpoints must beLike {
        case Some(Failure(blueeyes.json.serialization.Extractor.Invalid(_, None))) => ok
      }
    }

    "persist checkpoints between sessions" in TestZookeeperClient() { factory: ClientFactory =>
      val client1 = factory()
      val sc1 = newSystemCoordination(client1)

      // TODO: This has a side effect via createPersistent if the path doesn't exist. Refactor.
      sc1.loadYggCheckpoint("bifrost")

      val clock = VectorClock.empty.update(1,10).update(2,20)
      val in = YggCheckpoint(123, clock)
      sc1.saveYggCheckpoint("bifrost", in)

      sc1.close

      val client2 = factory()
      val sc2 = newSystemCoordination(client2)

      val result = sc2.loadYggCheckpoint("bifrost")

      result must beLike {
        case Some(Success(out)) =>
          in must_== out
      }
    }

    "distinguish between normal and abnormal bifrost/checkpoints shutdown" in {
      todo
    }
  }

  def newSystemCoordination(client: ZkClient) = new ZookeeperSystemCoordination(client, ServiceUID("test", "hostId", ""), true, None)

  type ClientFactory = () => ZkClient

  case class TestZookeeperClient(zkHosts: String = "127.0.0.1:2181") extends AroundOutside[ClientFactory] {

    private val clients = ListBuffer[ZkClient]()
    private val factory = () => {
      val client = new ZkClient(zkHosts, 1000)
      clients += client
      client
    }

    private def zookeeperAvailable(): Boolean = {
      try {
        val client = factory()
        var result = client != null
        client.close
        result
      } catch {
        case ex => false
      }
    }

    private def validatedFactory(): Option[ClientFactory] = {
      if(zookeeperAvailable()) Some(factory) else None
    }

    private val offline = new Skipped("SKIP - ZOOKEEPER NOT AVAILABLE")

    private def cleanup() {
      clients.foreach{ _.close }
      val client = factory()
      client.deleteRecursive("/precog-test")
      client.close
    }

    def around[T <% Result](t: =>T): Result = {
      validatedFactory().map{ _ =>
        try {
          t: Result
        } finally {
          cleanup
        }
      }.getOrElse(offline)
    }

    def outside: ClientFactory = factory
  }
}
