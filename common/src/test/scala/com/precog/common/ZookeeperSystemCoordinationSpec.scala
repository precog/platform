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
    "register relay agent" in zookeeperClient() { factory: ClientFactory =>
      val client = factory()
      val sc = newSystemCoordination(client)

      val result = sc.registerRelayAgent("test_agent", 10000)

      result must beLike {
        case Success(EventRelayState(0, 1, IdSequenceBlock(0, 1, 10000))) => ok 
      }
    }

    "renew relay agent" in zookeeperClient() { factory: ClientFactory =>
      val client = factory()
      val sc = newSystemCoordination(client)

      sc.registerRelayAgent("test_agent", 10000)
      val result = sc.renewEventRelayState("test_agent", 0L, 0, 10000)

      result must beLike {
        case Success(EventRelayState(0, 10001, IdSequenceBlock(0, 10001, 20000))) => ok 
      }
    } 

    "save and restore relay agent state" in zookeeperClient() { factory: ClientFactory =>
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

    "restore relay agent state" in zookeeperClient() { factory: ClientFactory =>
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

    "relay agent registration retries in case of stale registration" in zookeeperClient() { factory: ClientFactory =>
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

    "relay agent registration fails after reasonable attempt to detect stale registration" in zookeeperClient() { factory: ClientFactory =>
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

    "not load a missing checkpoint (checkpoints for new shards should be inserted manually via YggUtils)" in zookeeperClient() { factory: ClientFactory =>
      val client = factory()
      val sc = newSystemCoordination(client)
      
      val checkpoints = sc.loadYggCheckpoint("shard")

      checkpoints must beLike {
        case Some(Failure(blueeyes.json.xschema.Extractor.Invalid(_))) => ok
      }
    }

    "persist checkpoints between sessions" in zookeeperClient() { factory: ClientFactory =>
      val client1 = factory()
      val sc1 = newSystemCoordination(client1)
      
      // TODO: This has a side effect via createPersistent if the path doesn't exist. Refactor.
      sc1.loadYggCheckpoint("shard")

      val clock = VectorClock.empty.update(1,10).update(2,20)
      val in = YggCheckpoint(123, clock)
      sc1.saveYggCheckpoint("shard", in)
      
      sc1.close

      val client2 = factory()
      val sc2 = newSystemCoordination(client2)

      val result = sc2.loadYggCheckpoint("shard")
      
      result must beLike {
        case Some(Success(out)) => 
          in must_== out 
      }
    }

    "distinguish between normal and abnormal shard/checkpoints shutdown" in {
      todo
    }
  }

  def newSystemCoordination(client: ZkClient) = new ZookeeperSystemCoordination(client, ServiceUID("test", "hostId", ""), true) 
    
  type ClientFactory = () => ZkClient

  case class zookeeperClient(zkHosts: String = "127.0.0.1:2181") extends AroundOutside[ClientFactory] {

    private val clients = ListBuffer[ZkClient]()
    private val factory = () => {
      val client = new ZkClient(zkHosts, 10000)
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

