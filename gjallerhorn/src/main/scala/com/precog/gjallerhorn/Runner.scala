package com.precog.gjallerhorn

import dispatch._
import specs2._

trait Runner {
  def tasks(settings: Settings): List[Task]

  def main(args: Array[String]) {
    try {
      run(tasks(Settings.fromFile(new java.io.File("shard.out"))):_*)
    } finally {
      Http.shutdown()
    }
  }
}
