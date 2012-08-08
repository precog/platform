package com.precog.yggdrasil
package jdbm3

import com.precog.yggdrasil.table._

import blueeyes.json.JPath
import org.joda.time.DateTime

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

/**
 * This class is responsible for encoding and decoding a Seq[(ColumnRef,Column)]
 * into a byte array for serialization. It is *not* thread-safe.
 */
class ColumnCodec(bufferSize: Int = (16 * 1024)) {
  import CTypeMappings._

  private final val workBuffer = ByteBuffer.allocate(bufferSize)
  private final val DefaultCharset = "UTF-8"
  
  private final val FALSE_VALUE = 0.toByte
  private final val TRUE_VALUE = 1.toByte

  private def writeString(s: String) {
    // RLE Strings
    val bytes = s.getBytes(DefaultCharset)
    workBuffer.putInt(bytes.length)
    workBuffer.put(bytes)
  }

  private def writeBigDecimal(bd: BigDecimal) {
    writeString(bd.toString) // TODO: Figure out a sane way to serialize BigDecimal
  }

  private def writeBoolean(b: Boolean) {
    workBuffer.put(if (b) TRUE_VALUE else FALSE_VALUE)
  }

  private def readString(buffer: ByteBuffer): String = {
    // TODO: Could possibly be more efficient here with allocations
    val bytes = new Array[Byte](buffer.getInt())
    buffer.get(bytes)
    new String(bytes, DefaultCharset)
  }

  private def readBigDecimal(buffer: ByteBuffer): BigDecimal = {
    BigDecimal(readString(buffer))
  }

  private def readBoolean(buffer: ByteBuffer): Boolean = {
    buffer.get() match {
      case TRUE_VALUE  => true
      case FALSE_VALUE => false
      case invalid     => sys.error("Invalid boolean encoded value: " + invalid)
    }
  }

  def encode(values: Seq[CValue]): Array[Byte] = {
    workBuffer.clear()

    values.foreach { v => {
      workBuffer.put(flagFor(CType.of(v)))
      v match {
        case CString(cs)  => writeString(cs)                  
        case CBoolean(cb) => writeBoolean(cb)
        case CLong(cl)    => workBuffer.putLong(cl)
        case CDouble(cd)  => workBuffer.putDouble(cd)
        case CNum(cn)     => writeBigDecimal(cn)
        case CDate(cd)    => workBuffer.putLong(cd.getMillis)
        case CNull        => // NOOP, no value to write
        case CEmptyObject => // NOOP, no value to write
        case CEmptyArray  => // NOOP, no value to write
        case CUndefined   => // NOOP, no value to write
      }
    }}

    // Read bytes out of the bytebuffer into a new array
    val outBytes = new Array[Byte](workBuffer.position())
    workBuffer.flip()
    workBuffer.get(outBytes)

    outBytes
  }

  def encode(columns: Seq[(ColumnRef, Column)], row: Int, encodeRef: Boolean = false): Array[Byte] = {
    workBuffer.clear()

    columns.foreach {
      case (ref, column) => {
        if (encodeRef) {
          writeString(ref.selector.toString)
        }
        
        if (column.isDefinedAt(row)) {
          workBuffer.put(flagFor(ref.ctype))
          ref.ctype match {
            case CString      => writeString(column.asInstanceOf[StrColumn].apply(row))                  
            case CBoolean     => writeBoolean(column.asInstanceOf[BoolColumn].apply(row))                
            case CLong        => workBuffer.putLong(column.asInstanceOf[LongColumn].apply(row))          
            case CDouble      => workBuffer.putDouble(column.asInstanceOf[DoubleColumn].apply(row))      
            case CNum         => writeBigDecimal(column.asInstanceOf[NumColumn].apply(row))              
            case CDate        => workBuffer.putLong(column.asInstanceOf[DateColumn].apply(row).getMillis)
            case CNull        => // No value encoded
            case CEmptyObject => // No value encoded
            case CEmptyArray  => // No value encoded
            case CUndefined   => sys.error("Cannot encode an undefined column")
          }
        } else {
          workBuffer.put(FUNDEFINED)
        }
      }
    }

    // Read bytes out of the bytebuffer into a new array
    val outBytes = new Array[Byte](workBuffer.position())
    workBuffer.flip()
    workBuffer.get(outBytes)

    outBytes
  }

  def decodeWithRefs(input: Array[Byte]): Array[(String, CValue)] = decodeWithRefs(ByteBuffer.wrap(input))

  def decodeWithRefs(buffer: ByteBuffer): Array[(String, CValue)] = {
    var resultBuffer = ArrayBuffer[(String, CValue)]()

    while (buffer.hasRemaining()) {
      val selector = readString(buffer)

      resultBuffer.append((selector, (buffer.get() match {
        case FSTRING       => CString(readString(buffer))
        case FBOOLEAN      => CBoolean(readBoolean(buffer))
        case FLONG         => CLong(buffer.getLong())
        case FDOUBLE       => CDouble(buffer.getDouble())
        case FNUM          => CNum(readBigDecimal(buffer))
        case FDATE         => CDate(new DateTime(buffer.getLong()))
        case FNULL         => CNull
        case FEMPTYOBJECT  => CEmptyObject
        case FEMPTYARRAY   => CEmptyArray
        case FUNDEFINED    => CUndefined
        case invalid       => sys.error("Invalid format flag: " + invalid)
      })))
    }

    resultBuffer.toArray
  }

  def decodeToArrayColumns(input: Array[Byte], row: Int, columns: Array[ArrayColumn[_]]) {
    decodeToArrayColumns(ByteBuffer.wrap(input), row, columns)
  }

  /**
   * Decode the given byte buffer, storing its values in the proper ArrayColumns
   * @param buffer The buffer to read from. Must be ready for reads (position == 0)
   */
  def decodeToArrayColumns(buffer: ByteBuffer, row: Int, columns: Array[ArrayColumn[_]]) {
    var columnIndex = 0

    while (buffer.hasRemaining()) {
      buffer.get() match {
        case FSTRING       => columns(columnIndex).asInstanceOf[ArrayStrColumn].update(row, readString(buffer))
        case FBOOLEAN      => columns(columnIndex).asInstanceOf[ArrayBoolColumn].update(row, readBoolean(buffer))
        case FLONG         => columns(columnIndex).asInstanceOf[ArrayLongColumn].update(row, buffer.getLong())
        case FDOUBLE       => columns(columnIndex).asInstanceOf[ArrayDoubleColumn].update(row, buffer.getDouble())
        case FNUM          => columns(columnIndex).asInstanceOf[ArrayNumColumn].update(row, readBigDecimal(buffer))
        case FDATE         => columns(columnIndex).asInstanceOf[ArrayDateColumn].update(row, new DateTime(buffer.getLong()))
        case FNULL         => columns(columnIndex).asInstanceOf[MutableNullColumn].update(row, true)
        case FEMPTYOBJECT  => columns(columnIndex).asInstanceOf[MutableEmptyObjectColumn].update(row, true)
        case FEMPTYARRAY   => columns(columnIndex).asInstanceOf[MutableEmptyArrayColumn].update(row, true)          
        case FUNDEFINED    => // NOOP, array/mutable columns start fully undefined                                  
        case invalid       => sys.error("Invalid format flag: " + invalid)                                          
      }                                                                                                             
      columnIndex += 1                                                                                              
    }                                                                                                               
  }                                                                                                                 
}                                                                                                                   
                                                                                                                    
                                                                                                                    
