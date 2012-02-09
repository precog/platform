package com.precog
package yggdrasil

import org.streum.configrity._

import common.Config

import java.io.File

trait YggConfigComponent {
  type YggConfig 

  def yggConfig: YggConfig
}

trait BaseConfig extends Config {
  private val localDefaults = Configuration.parse("""
    precog {
      storage {
        root = ./data
        sortBufferSize = 100000
      }
    }
 """, io.BlockFormat)

  lazy private val cfg = localDefaults ++ config 

  lazy val rootDir = new File(cfg[String]("precog.storage.root"))
  
  lazy val dataDir = new File(rootDir, "data")
  lazy val cacheDir = new File(rootDir, "cache")
  lazy val scratchDir = new File(rootDir, "scratch")

  def newWorkDir = {
    if(!scratchDir.exists) scratchDir.mkdirs
    val tempFile = File.createTempFile("ygg", "workdir", scratchDir)
    tempFile.delete
    tempFile.mkdir
    tempFile
  }

  lazy val sortBufferSize: Int = cfg[Int]("precog.storage.sortBufferSize")
}
