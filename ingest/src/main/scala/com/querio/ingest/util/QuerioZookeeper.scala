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
