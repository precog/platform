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
