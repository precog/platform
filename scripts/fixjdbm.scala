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
import com.precog.yggdrasil._
import com.precog.yggdrasil.jdbm3._
import com.precog.yggdrasil.table._

import java.io.File

import org.apache.jdbm._

import scala.collection.JavaConverters._

(new File("fixed")).mkdirs()

val src = DBMaker.openFile("byIdentity").make()
val dst = DBMaker.openFile("fixed/byIdentity").make()

val mapName = "byIdentityMap"

val si = src.getTreeMap(mapName).asInstanceOf[java.util.Map[Array[Byte], Array[Byte]]]

println("Input size = " + si.size)

val keyColRefs = Seq(ColumnRef(".key[0]", CLong))

val keyFormat = RowFormat.IdentitiesRowFormatV1(keyColRefs)

val di = dst.createTreeMap(mapName, SortingKeyComparator(keyFormat, true), ByteArraySerializer, ByteArraySerializer)

si.entrySet.iterator.asScala.foreach {
  e => di.put(e.getKey, e.getValue.drop(2))
}

dst.commit()

src.close()
dst.close()
