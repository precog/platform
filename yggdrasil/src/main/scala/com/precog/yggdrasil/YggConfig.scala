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
package com.precog
package yggdrasil

import org.streum.configrity._

import common.Config

import java.io.File

trait YggConfigComponent {
  type YggConfig 

  val yggConfig: YggConfig
}

trait BaseConfig extends Config {
  private val localDefaults = Configuration.parse("""
    precog {
      storage {
        root = ./data
        sortBufferSize = 100000
      }
      evaluator {
        timeout {
          fm = 30
          projection = 30
        }
      }
    }
 """, io.BlockFormat)

  lazy private val cfg = localDefaults ++ config 

  lazy val rootDir = new File(cfg[String]("precog.storage.root"))
  
  lazy val dataDir = new File(rootDir, "data")
  lazy val archiveDir = new File(rootDir, "archive")
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
