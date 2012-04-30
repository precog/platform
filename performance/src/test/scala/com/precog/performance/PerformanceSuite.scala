package com.precog.performance

import org.specs2.mutable.Specification

import com.precog.yggdrasil.leveldb._

import org.iq80.leveldb._
import org.fusesource.leveldbjni.JniDBFactory._

import java.io.File
import java.nio.ByteBuffer

class PerformanceSuite 
  extends LeveldbPerformanceSpec 
  with RoutingPerformanceSpec 
  with YggdrasilPerformanceSpec 
