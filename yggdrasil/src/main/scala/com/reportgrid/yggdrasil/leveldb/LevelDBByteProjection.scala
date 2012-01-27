package com.reportgrid.yggdrasil
package leveldb

import com.reportgrid.util.Bijection._
import java.nio.ByteBuffer

private[leveldb] class LevelDBWriteBuffer(length: Int) {
  
  //println("length = " + length)

  val trueByte: Byte = 0x01
  val falseByte: Byte = 0x00
  val nullByte: Byte = 0xff.toByte
  
  private final val buf = ByteBuffer.allocate(length)
  def writeIdentity(id: Identity): Unit = buf.putLong(id)
  def writeValue(colDesc: ColumnDescriptor, v: CValue) = v.fold[Unit](
    str     = s => {
      val sbytes = s.getBytes("UTF-8")
      colDesc.valueType.format match {
        case FixedWidth(w) => buf.put(sbytes, 0, w)
        case LengthEncoded => buf.putInt(sbytes.length).put(sbytes)
      }
    },
    bool    = b => buf.put(if (b) trueByte else falseByte), 
    int     = i => buf.putInt(i), 
    long    = l => buf.putLong(l),
    float   = f => buf.putFloat(f), 
    double  = d => buf.putDouble(d), 
    num     = d => {
      val dbytes = d.as[Array[Byte]]
      buf.putInt(dbytes.length).put(dbytes)
    },
    emptyObj = (), emptyArr = (), 
    nul = colDesc.valueType match {
      case SStringFixed(width)    => buf.put(Array.fill[Byte](width)(0x00))
      case SStringArbitrary       => buf.putInt(0)
      case SBoolean               => buf.put(nullByte)
      case SInt                   => buf.putInt(Int.MaxValue)
      case SLong                  => buf.putLong(Long.MaxValue)
      case SFloat                 => buf.putFloat(Float.MaxValue)
      case SDouble                => buf.putDouble(Double.MaxValue)
      case SDecimalArbitrary      => buf.putInt(0)
      case _                      => ()
    }
  )

  def toArray: Array[Byte] = buf.array
}

private[leveldb] class LevelDBReadBuffer(arr: Array[Byte]) {

  private final val buf = ByteBuffer.wrap(arr)

  def readIdentity(): Long = buf.getLong

  def readValue(colDesc: ColumnDescriptor): CValue = {
    sys.error("todo")
//    colDesc.valueType match {
//      case SStringFixed(width)    => buf.put(Array.fill[Byte](width)(0x00))
//      case SStringArbitrary       => buf.putInt(0)
//      case SBoolean               => buf.put(nullByte)
//      case SInt                   => buf.putInt(Int.MaxValue)
//      case SLong                  => buf.putLong(Long.MaxValue)
//      case SFloat                 => buf.putFloat(Float.MaxValue)
//      case SDouble                => buf.putDouble(Double.MaxValue)
//      case SDecimalArbitrary      => buf.putInt(0)
//      case _                      => ()
//    }
  }

}

trait LevelDBByteProjection extends ByteProjection {
  private val incompatible = (_: Any) => sys.error("Column values incompatible with projection descriptor.")

  def project(identities: Identities, cvalues: Seq[CValue]): (Array[Byte], Array[Byte]) = {
    lazy val valueWidths = descriptor.columns.map(_.valueType.format) zip cvalues map {
      case (FixedWidth(w), sv) => w
      case (LengthEncoded, sv) => 
        sv.fold[Int](
          str     = s => s.getBytes("UTF-8").length + 4,
          bool    = incompatible, int     = incompatible, long    = incompatible,
          float   = incompatible, double  = incompatible, 
          num     = _.as[Array[Byte]].length + 4,
          emptyObj = incompatible(()), emptyArr = incompatible(()), nul = incompatible(())
        )
    }
    //println("valueWidths = " + valueWidths)

    val (usedIdentities, usedValues, indexWidth) = descriptor.sorting.foldLeft((Set.empty[Int], Set.empty[Int], 0)) { 
      case ((ids, values, width), (col, ById)) => 
        (ids + descriptor.indexedColumns(col), values, width + 8)

      case ((ids, values, width), (col, ByValue)) => 
        val valueIndex = descriptor.columns.indexOf(col)
        (ids, values + valueIndex, width + valueWidths(valueIndex))

      case ((ids, values, width), (col, ByValueThenId)) => 
        val valueIndex = descriptor.columns.indexOf(col)
        (ids + descriptor.indexedColumns(col), values + valueIndex, width + valueWidths(valueIndex) + 8)
    }
    //println("(usedIdentities, usedValues, indexWidth) = " + (usedIdentities, usedValues, indexWidth))

    // all of the identities must be included in the key; also, any values of columns that
    // use by-value ordering must be included in the key. 
    val indexBuffer = new LevelDBWriteBuffer(indexWidth + ((identities.size - usedIdentities.size) * 8))
    descriptor.sorting.foreach {
      case (col, ById)          => indexBuffer.writeIdentity(identities(descriptor.indexedColumns(col)))
      case (col, ByValue)       => indexBuffer.writeValue(col, cvalues(descriptor.indexedColumns(col)))
      case (col, ByValueThenId) =>
        indexBuffer.writeValue(col, cvalues(descriptor.indexedColumns(col)))
        indexBuffer.writeIdentity(identities(descriptor.indexedColumns(col)))
    }

    identities.zipWithIndex.foreach {
      case (id, i) => if (!usedIdentities.contains(i)) indexBuffer.writeIdentity(id)
      case _ =>
    }

    val valuesBuffer = new LevelDBWriteBuffer(valueWidths.zipWithIndex collect { case (w, i) if !usedValues.contains(i) => w } sum)
    (cvalues zip descriptor.columns).zipWithIndex.foreach {
      case ((v, col), i) if !usedValues.contains(i) => valuesBuffer.writeValue(col, v)
      case _ => 
    }

    (indexBuffer.toArray, valuesBuffer.toArray)
  }

  def unproject[E](keyBytes: Array[Byte], valueBytes: Array[Byte])(f: (Identities, Seq[CValue]) => E): E = {
    sys.error("todo")
  }
}

// vim: set ts=4 sw=4 et:
