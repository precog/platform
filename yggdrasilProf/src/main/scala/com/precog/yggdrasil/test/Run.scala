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
package com.precog.yggdrasil
package test

import com.precog.yggdrasil.util.IdSourceConfig

object Run extends com.precog.yggdrasil.table.GrouperSpec[YId] with YIdInstances {
  type YggConfig = IdSourceConfig
  val yggConfig = new IdSourceConfig {
    val idSource = new IdSource {
      private val source = new java.util.concurrent.atomic.AtomicLong
      def nextId() = source.getAndIncrement
    }
  }

  def main(argv: Array[String]) = {
    testCtrPartialJoinOr(
      Stream((-954410459,Some(0)), (-1,Some(2007696701)), (2105675940,Some(-1245674830)), (-1582587372,Some(1940093023)), (63198658,Some(2068956190)), (0,Some(-189150978)), (2000592976,Some(-222301652)), (523154377,Some(0)), (-2147483648,Some(1632775270)), (1092038023,Some(1819439617)), (-2147483648,Some(2147483647)), (0,Some(0)), (2147483647,Some(0)), (-1143657189,Some(-2147483648)), (-1958852329,Some(2147483647)), (-2147483648,Some(608866931)), (-273338630,Some(-2147483648)), (-2147483648,Some(-1841559997)), (-2147483648,Some(1601378038)), (0,Some(-1)), (1,Some(1)), (-670756012,Some(-106440741)), (-2147483648,Some(-431649434)), (0,Some(585196920)), (0,Some(143242157)), (2147483647,Some(0)), (-1002181171,Some(2147483647)), (260767290,Some(2147483647)), (2147483647,Some(0)), (1502519219,Some(-80993454)), (-2147483648,Some(1)), (26401216,Some(1737006538)), (459053133,Some(1)), (1,Some(222440292)), (2147483647,Some(-1)), (-785490772,Some(2147483647)), (-1519510933,Some(1)), (1064945303,Some(2015037890)), (2147483647,Some(-1888515244)), (-2147483648,Some(0)), (-1782288738,Some(-2147483648)), (-1243866137,Some(-2036899743)), (2147483647,Some(-2147483648)), (152217775,Some(1)), (-1,Some(1822038570)), (-557295510,Some(-2147483648)), (0,Some(0)), (-1389729666,Some(407111520)), (0,Some(1110392883)), (-2042103283,Some(-1366550515)), (-1309507483,Some(-2147483648)), (2147483647,Some(0)), (1322668865,Some(1)), (1,Some(1)), (1296673327,Some(341152609)), (1040120825,Some(-1731488506)), (-951605740,Some(1)), (690140640,Some(-1783450717)), (1395849695,Some(768982688)), (-1,Some(-894395447)), (2147483647,Some(2147483647)), (-1,Some(-2016297234)), (-1416825502,Some(-2147483648)), (1727813995,Some(1)), (-1178284872,Some(-2147483648)), (2147483647,Some(-1468556846)), (-361436734,Some(0)), (960146451,Some(-2147483648)), (-2147483648,Some(-2147483648)), (973715803,Some(603648248)), (2147483647,Some(0)), (-2147483648,Some(-36955603)), (2005706222,Some(-242403982)), (-1274227445,Some(1156421302)), (-2147483648,Some(385347685)), (-2147483648,Some(926114223)), (1690927871,Some(1)), (-330611474,Some(-2147483648)), (-1801526113,Some(922619077)), (-2147483648,Some(-1903319530)), (2147483647,Some(0))),
      Stream(0, 0, 0, 1, 1, 434608913, 193294286, 0, -1921860406, 2147483647, -2147483648, 1, -1, 0, -2147483648, 0, -113276442, -1564947365, 2147483647, -54676151, -1, 49986682, -391210112, 1, -1, 2147483647, 0, -1, 0, 0, 2147483647, -225140804, 1245119802, 1, -548778232, -1138847365, 1, 73483948, 0, -1, -996046474, -695581403, 2147483647, -2147483648, -1, 1563916971, -2147483648, 0, 1, 607908889, -2009071663, -1382431435, 778550183, 2147483647, -2147483648, 0, -1)
    )
  }
}


// vim: set ts=4 sw=4 et:
