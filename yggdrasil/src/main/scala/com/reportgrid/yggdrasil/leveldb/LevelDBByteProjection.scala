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

  def readValue(valueType: ColumnType): CValue = {
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

    type ElementFormat = LevelDBReadBuffer => CValue

    def eFormat(t: ColumnType): ElementFormat = _.readValue(t)
    
    def indexFormat: List[ElementFormat] = {

      val (initial, unused) = descriptor.sorting.foldLeft((List[ElementFormat](), 0.until(descriptor.identities).toList)) {
        case ((acc, ids), (col, ById))          => (acc :+ eFormat(SLong), ids.filter( _ != descriptor.indexedColumns(col)))
        case ((acc, ids), (col, ByValue))       => (acc :+ eFormat(col.valueType), ids)
        case ((acc, ids), (col, ByValueThenId)) => (acc ++ (eFormat(col.valueType) :: eFormat(SLong) :: Nil), ids.filter( _ != descriptor.indexedColumns(col)))
      }

      unused.foldLeft(initial) {
        case (acc, idx) => acc :+ eFormat(SLong)
      }
    }

    def valueFormat: List[ElementFormat] = 
      descriptor.sorting.foldLeft(descriptor.columns) {
        case (acc, (_, ById)) => acc
        case (acc, (col, _))  => acc.filter( _ != col)
      } map { col => eFormat(col.valueType) }

    def extractIdentities(indexMembers: Seq[CValue]): Identities = {
      val (initial, unused) = descriptor.sorting.foldLeft((List[Int](), 0.until(descriptor.identities).toList)) {
        case ((acc, ids), (col, ById))          => (acc :+ descriptor.indexedColumns(col), ids.filter( _ != descriptor.indexedColumns(col)))
        case ((acc, ids), (_, ByValue))         => (acc :+ -1, ids)
        case ((acc, ids), (col, ByValueThenId)) => (acc ++ (descriptor.indexedColumns(col) :: -1 :: Nil), ids.filter( _ != descriptor.indexedColumns(col)))
      }

      val mapping = unused.foldLeft(initial) {
        case (acc, el) => acc :+ el
      }.zipWithIndex.filter( _._1 == -1 ).sortBy( _._1 ).map( _._2 )

      mapping.foldLeft( Vector[Long]() ) {
        case (acc, source) => indexMembers(source) match {
          case CLong(l) => acc :+ l
          case _        => sys.error("Invalid index type mapping")
        }
      }
    }

    def extractValues(indexMembers: Seq[CValue], valueMembers: Seq[CValue]): Seq[CValue] = {
      
      val (_, values) = descriptor.sorting.foldLeft((indexMembers, List[(Int, CValue)]())) {
        case ((id :: tail, acc), (_, ById))                     => (tail, acc)
        case ((value :: tail, acc), (col, ByValue))             => (tail, acc :+ (descriptor.columns.indexOf(col) -> value))
        case ((value :: id :: tail, acc), (col, ByValueThenId)) => (tail, acc :+ (descriptor.columns.indexOf(col) -> value))
      }

      descriptor.sorting.foldLeft(descriptor.columns) {
        case (acc, (_, ById)) => acc
        case (acc, (col, _))  => acc.filter( _ != col)
      }.zipWithIndex.foldLeft(values) {
        case (acc, (col, index)) => acc :+ (index -> valueMembers(index)) 
      }.sortBy(_._1).map(_._2)
    
    }

    val indexBuffer = new LevelDBReadBuffer(keyBytes)
    val valueBuffer = new LevelDBReadBuffer(valueBytes)

    val indexMembers = indexFormat.map( _(indexBuffer) )
    val valueMembers = valueFormat.map( _(valueBuffer) )

    val identities = extractIdentities(indexMembers)
    val values = extractValues(indexMembers, valueMembers)
    
    f(identities, values)
  }
}

// vim: set ts=4 sw=4 et:
