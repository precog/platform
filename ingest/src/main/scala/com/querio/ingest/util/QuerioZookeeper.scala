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
package com.querio.ingest.util 

import java.util.ArrayList
import org.apache.zookeeper._

class QuerioZookeeper(zkHosts: String, 
                      basePaths: Seq[String],
                      prefix: String) extends Watcher {
  
  lazy val basePath = "/" + basePaths.mkString("/")  
  lazy val producerIdPath = basePath + "/" + prefix  
  
  private val zk = new ZooKeeper(zkHosts, 3000, this);
  
  def acquireProducerId(): Int = {
    val path = createPath(producerIdPath, CreateMode.PERSISTENT_SEQUENTIAL)
    path.substring(path.length - 10).toInt
  }

  def setup() {
    basePaths.foldLeft("/")((parent, child) => {
      val path = parent + child
      createPath(path, CreateMode.PERSISTENT)
      path + "/"
    })
  }
  
  private def createPath(path: String, createMode: CreateMode): String = {
    if(zk.exists(path, false) == null) {
      zk.create(path, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode)
    } else {
      path
    }
  }
  
  def stop() {
    zk.close()
  }

  override def process(event: WatchedEvent) { }

}

object QuerioZookeeper {

  val prefix = "prodId-"
  val paths = List("com", "querio", "ingest", "v1", "producer", "id")

  val testHosts = "localhost:2181"
  def testQuerioZookeeper = new QuerioZookeeper(testHosts, "test" :: paths, prefix)

  val prodHosts = "localhost:2181"
  def prodQuerioZookeeper = new QuerioZookeeper(prodHosts, paths, prefix)
}
