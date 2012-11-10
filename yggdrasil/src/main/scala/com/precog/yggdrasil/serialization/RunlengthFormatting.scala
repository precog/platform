package com.precog.yggdrasil
package serialization

import com.precog.yggdrasil.SValue._
import com.precog.yggdrasil.CType._

import blueeyes.json._
import blueeyes.json.JPath._

import java.io._
import scala.annotation.tailrec
import scalaz.effect._
import scalaz.syntax.monad._

trait RunlengthFormatting[A] {
  final val HeaderFlag = Int.MinValue
  final val ValueFlag = Int.MinValue + 1

  type Header

  def headerFor(value: A): Header

  def writeHeader(out: DataOutputStream, header: Header): Unit
  def writeRecord(out: DataOutputStream, value: A, header: Header): Unit 

  def readHeader(in: DataInputStream): Header
  def readRecord(in: DataInputStream, header: Header): A
}

class WrapRunlengthFormatting[A](val delegate: RunlengthFormatting[A]) extends RunlengthFormatting[A] {
  type Header = delegate.Header

  def headerFor(value: A): Header = delegate.headerFor(value)

  def writeHeader(out: DataOutputStream, header: Header): Unit = delegate.writeHeader(out, header)
  def writeRecord(out: DataOutputStream, value: A, header: Header): Unit = delegate.writeRecord(out, value, header)

  def readHeader(in: DataInputStream): Header = delegate.readHeader(in)
  def readRecord(in: DataInputStream, header: Header): A = delegate.readRecord(in, header)
}

trait SValueRunlengthFormatting extends RunlengthFormatting[SValue] with SValueFormatting {
  case class Header(structure: Seq[(JPath, CType)])

  def headerFor(value: SValue) = Header(value.structure)

  def writeHeader(out: DataOutputStream, header: Header): Unit = {
    writeStructure(out, header.structure)
  }

  def writeRecord(out: DataOutputStream, sv: SValue, header: Header): Unit = {
    writeValue(out, sv, header.structure)
  }

  def readHeader(in: DataInputStream): Header = Header(readStructure(in))
  def readRecord(in: DataInputStream, header: Header) = readValue(in, header.structure)
}

trait SEventRunlengthFormatting extends RunlengthFormatting[SEvent] 
with SValueFormatting with IdentitiesFormatting {
  case class Header(idCount: Int, structure: Seq[(JPath, CType)])

  def headerFor(value: SEvent) = Header(value._1.length, value._2.structure)

  def writeHeader(out: DataOutputStream, header: Header): Unit = {
    out.writeInt(header.idCount)
    writeStructure(out, header.structure)
  }

  def writeRecord(out: DataOutputStream, sv: SEvent, header: Header): Unit = {
    writeIdentities(out, sv._1)
    writeValue(out, sv._2, header.structure)
  }

  def readHeader(in: DataInputStream): Header = {
    Header(in.readInt(), readStructure(in))
  }

  def readRecord(in: DataInputStream, header: Header) = {
    (readIdentities(in, header.idCount), readValue(in, header.structure))
  }
}

trait GroupRunlengthFormatting extends RunlengthFormatting[(SValue, Identities, SValue)] 
with SValueFormatting with IdentitiesFormatting {
  case class Header(keyStructure: Seq[(JPath, CType)], idCount: Int, valueStructure: Seq[(JPath, CType)])

  def headerFor(value: (SValue, Identities, SValue)) = Header(value._1.structure, value._2.length, value._3.structure)

  def writeHeader(out: DataOutputStream, header: Header): Unit = {
    writeStructure(out, header.keyStructure)
    out.writeInt(header.idCount)
    writeStructure(out, header.valueStructure)
  }

  def writeRecord(out: DataOutputStream, sv: (SValue, Identities, SValue), header: Header): Unit = {
    writeValue(out, sv._1, header.keyStructure)
    writeIdentities(out, sv._2)
    writeValue(out, sv._3, header.valueStructure)
  }

  def readHeader(in: DataInputStream): Header = {
    Header(readStructure(in), in.readInt(), readStructure(in))
  }

  def readRecord(in: DataInputStream, header: Header) = {
    (readValue(in, header.keyStructure), readIdentities(in, header.idCount), readValue(in, header.valueStructure))
  }
}

// vim: set ts=4 sw=4 et:
