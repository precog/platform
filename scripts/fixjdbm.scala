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
