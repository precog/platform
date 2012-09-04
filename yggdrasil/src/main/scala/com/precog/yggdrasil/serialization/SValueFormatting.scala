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
package serialization

import bijections._

import com.precog.common.VectorCase
import com.precog.yggdrasil.SValue._
import com.precog.yggdrasil.CType._
import com.precog.util.Bijection._

import blueeyes.json._
import blueeyes.json.JPath._

import java.io._
import scala.annotation.tailrec
import scalaz.effect._
import scalaz.syntax.monad._


trait SValueFormatting {
  def writeStructure(out: DataOutputStream, structure: Seq[(JPath, CType)]): Unit
  def readStructure(in: DataInputStream): Seq[(JPath, CType)]

  def writeValue(out: DataOutputStream, sv: SValue, structure: Seq[(JPath, CType)]): Unit
  def readValue(in: DataInputStream, structure: Seq[(JPath, CType)]): SValue
}

trait IdentitiesFormatting {
  def writeIdentities(out: DataOutputStream, id: Identities): Unit
  def readIdentities(in: DataInputStream, length: Int): Identities
}

trait BinarySValueFormatting extends SValueFormatting with IdentitiesFormatting {
  def writeStructure(out: DataOutputStream, structure: Seq[(JPath, CType)]): Unit = {
    out.writeInt(structure.size)
    structure foreach {
      case (sel, valType) => {       
        out.writeUTF(sel.toString)
        out.writeUTF(nameOf(valType))
      }
    }
  }

  def readStructure(in: DataInputStream): Seq[(JPath, CType)] = {
    @tailrec def loop(in: DataInputStream, acc: Seq[(JPath, CType)], i: Int): Seq[(JPath, CType)] = {
      if (i > 0) {
        val selector = JPath(in.readUTF())
        CType.fromName(in.readUTF()) match {
          case Some(ctype) => loop(in, acc :+ ((selector, ctype)), i - 1)
          case None        => sys.error("Memoization header corrupt: unable to read back column type indicator.")
        }
      } else {
        acc
      }
    }

    val columns = in.readInt()
    loop(in, Vector.empty[(JPath, CType)], columns)
  }

  def writeValue(out: DataOutputStream, sv: SValue, structure: Seq[(JPath, CType)]): Unit = {
    for ((selector, ctype) <- structure) {
      val leaf = (sv \ selector) 
      leaf match {
        case Some(SString(str))  => out.writeUTF(str)
        case Some(STrue)         => out.writeBoolean(true)
        case Some(SFalse)        => out.writeBoolean(false)
        case Some(SDecimal(num)) => 
          val bytes = biject(num).as[Array[Byte]]
          assert(bytes.length > 0)

          out.writeInt(bytes.length)
          out.write(bytes, 0, bytes.length)
        
        case Some(SNull) => out.writeInt(0)
        case Some(SArray(values)) if(values.size == 0) => out.writeInt(0)
        case Some(SObject(fields)) if(fields.size == 0) => out.writeInt(0)
        case _ => sys.error("Value structure " + sv.structure + " for value " + sv.toString + " does not correspond to write header " + structure)
      }
    }
  }

  def readValue(in: DataInputStream, structure: Seq[(JPath, CType)]): SValue = {
    structure.foldLeft(Option.empty[SValue]) {
      case (None     , (JPath.Identity, ctype)) => {
        val back = readColumn(in, ctype)
        if (back eq null) {
          ctype match {
            case CEmptyObject => Some(SObject.Empty)
            case CEmptyArray => Some(SArray.Empty)
            case _ => Some(SNull)
          }
        } else {
          Some(SValue.fromCValue(back))
        }
      }
    
      case (None     , (jpath, ctype))          => 
        (jpath.nodes : @unchecked) match {
          case JPathIndex(_) :: xs => SArray(Vector()).set(jpath, readColumn(in, ctype))
          case JPathField(_) :: xs => SObject(Map()).set(jpath, readColumn(in, ctype))
        }
      case (Some(obj), (JPath.Identity, ctype)) => sys.error("Illegal data header: multiple values at a selector root.")
      case (Some(obj), (jpath, ctype))          => obj.set(jpath, readColumn(in, ctype))
    } getOrElse {
      SNull
    }
  }

  private def readColumn(in: DataInputStream, ctype: CType): CValue = {
    (ctype : @unchecked) match {
  //    case CStringFixed(_)   => CString(in.readUTF())
      case CString           => CString(in.readUTF())
      case CBoolean          => CBoolean(in.readBoolean())
  //    case CInt              => CInt(in.readInt())
  //    case CLong             => CLong(in.readLong())
  //    case CFloat            => CFloat(in.readFloat())
  //    case CDouble           => CDouble(in.readDouble())
      case CLong | CDouble | CNum => 
        val length = in.readInt()
        assert(length > 0)

        val bytes: Array[Byte] = new Array(length)
        in.readFully(bytes)
        CNum(biject(bytes).as[BigDecimal])

      case CNull => 
        assert(in.readInt() == 0)
        null
      
      case CEmptyObject => 
        assert(in.readInt() == 0)
        null
      
      case CEmptyArray => 
        assert(in.readInt() == 0)
        null
    }
  }

  def writeIdentities(out: DataOutputStream, id: Identities): Unit = {
    id.foreach(out.writeLong(_))
  }

  def readIdentities(in: DataInputStream, length: Int): Identities = {
    @tailrec def loop(in: DataInputStream, acc: VectorCase[Long], i: Int): Identities = {
      if (i > 0) {
        loop(in, acc :+ in.readLong(), i - 1)
      } else {
        acc
      }
    }

    loop(in, VectorCase.empty[Long], length)
  }
}
// vim: set ts=4 sw=4 et:
