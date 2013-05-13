package com.precog.bytecode

import java.nio.ByteBuffer
import java.nio.BufferUnderflowException
import scala.annotation.tailrec

trait Reader extends Instructions {
  import instructions._
  
  def read(buffer: ByteBuffer): Vector[Instruction]
}

trait BytecodeReader extends Reader {
  import instructions._
  import Function._
  import library._

  private lazy val stdlibMorphism1Ops: Map[Int, BuiltInMorphism1] = libMorphism1.map(op => op.opcode -> BuiltInMorphism1(op))(collection.breakOut)
  private lazy val stdlibMorphism2Ops: Map[Int, BuiltInMorphism2] = libMorphism2.map(op => op.opcode -> BuiltInMorphism2(op))(collection.breakOut)
  private lazy val stdlib1Ops: Map[Int, BuiltInFunction1Op] = lib1.map(op => op.opcode -> BuiltInFunction1Op(op))(collection.breakOut)
  private lazy val stdlib2Ops: Map[Int, BuiltInFunction2Op] = lib2.map(op => op.opcode -> BuiltInFunction2Op(op))(collection.breakOut)
  private lazy val stdlibReductionOps: Map[Int, BuiltInReduction] = libReduction.map(red => red.opcode -> BuiltInReduction(red))(collection.breakOut)
  
  def read(buffer: ByteBuffer): Vector[Instruction] = {
    val version = buffer.getInt()
    
    val major = (version >> 24) & 0x7F
    val minor = (version >> 16) & 0xFF
    val release = version & 0xFFFF
    
    // TODO check version compatibility
    
    val table = readSymbolTable(buffer)
    readStream(buffer, table, Vector())
  }
  
  @tailrec
  private[this] def readStream(buffer: ByteBuffer, table: Map[Int, ByteBuffer], acc: Vector[Instruction]): Vector[Instruction] = {
    val opcode = try {
      Some(buffer.getLong())
    } catch {
      case _: BufferUnderflowException => None
    }
    
    val instr = opcode flatMap { code =>
      lazy val parameter = (code & 0xFFFFFFFF).toInt
      
      lazy val tableEntry = table get parameter
      
      lazy val lineInfo = tableEntry flatMap readLineInfo
      
      lazy val string = tableEntry flatMap readString
      
      lazy val num = tableEntry flatMap readNum
      
      lazy val tableInt = tableEntry flatMap readInt

      lazy val unOp: Option[UnaryOperation] = (code & 0xFF) match {
        case 0x40 => Some(Comp)
        case 0x41 => Some(Neg)
        case 0x60 => Some(New)
        
        case 0x61 => Some(WrapArray)

        case 0xB0 => stdlib1Ops.get(((code >> 8) & 0xFFFFFF).toInt)
        
        case _ => None
      }
      
      lazy val binOp: Option[BinaryOperation] = (code & 0xFF) match {
        case 0x00 => Some(Add)
        case 0x01 => Some(Sub)
        case 0x02 => Some(Mul)
        case 0x03 => Some(Div)
        case 0x04 => Some(Mod)
        case 0x05 => Some(Pow)
        
        case 0x10 => Some(Lt)
        case 0x11 => Some(LtEq)
        case 0x12 => Some(Gt)
        case 0x13 => Some(GtEq)
        
        case 0x20 => Some(Eq)
        case 0x21 => Some(NotEq)
        
        case 0x30 => Some(Or)
        case 0x31 => Some(And)
        
        case 0x60 => Some(WrapObject)
        
        case 0x80 => Some(JoinObject)
        case 0x81 => Some(JoinArray)
        
        case 0x8A => Some(ArraySwap)
        
        case 0xA0 => Some(DerefObject)
        case 0xA1 => Some(DerefMetadata)
        case 0xA2 => Some(DerefArray)

        case 0xB1 => stdlib2Ops.get(((code >> 8) & 0xFFFFFF).toInt)
        
        case _ => None
      }
      
      lazy val reduction = (code & 0xFF) match {
        case 0xC0 => stdlibReductionOps.get(((code >> 8) & 0xFFFFFF).toInt)

        case _ => None
      }      

      lazy val morphism1 = (code & 0xFF) match {
        case 0xC1 => stdlibMorphism1Ops.get(((code >> 8) & 0xFFFFFF).toInt)

        case _ => None
      }

      lazy val morphism2 = (code & 0xFF) match {
        case 0xC2 => stdlibMorphism2Ops.get(((code >> 8) & 0xFFFFFF).toInt) 

        case _ => None
      }

      lazy val depth = ((code >> 32) & 0xFFFFFF).toShort
      
      lazy val instruction = ((code >> 56) & 0xFF) match {
        // operative instructions
        
        case 0x00 => unOp map Map1
        case 0x01 => binOp map Map2Match
        case 0x02 => binOp map Map2Cross
        
        case 0x08 => reduction map Reduce
        case 0x09 => morphism1 map Morph1
        case 0x19 => morphism2 map Morph2
        
        case 0x10 => Some(Assert)
        
        case 0x12 => Some(IUnion)
        case 0x13 => Some(IIntersect)

        case 0x15 => Some(SetDifference)
        
        case 0x14 => Some(FilterMatch)
        case 0x16 => Some(FilterCross)
        
        case 0x1A => Some(Group(parameter))
        case 0x1B => Some(MergeBuckets((code & 0x01) == 1))
        case 0x1C => Some(KeyPart(parameter))
        case 0x1D => Some(Extra)
        
        case 0x1E => Some(Split)
        case 0x1F => Some(Merge)

        case 0x03 => Some(Distinct)
        
        // manipulative instructions
        
        case 0x20 => Some(Dup)
        case 0x21 => Some(Drop)
        case 0x28 => tableInt map { _.toInt } map Swap     // TODO is this *supposed* to be like so?
        
        case 0x2A => lineInfo map tupled(Line)
        
        // introductive instructions
        
        case 0x40 => Some(LoadLocal)
        
        case 0x80 => string map PushString
        case 0x81 => num map PushNum
        case 0x82 => Some(PushTrue)
        case 0x83 => Some(PushFalse)
        case 0x84 => Some(PushObject)
        case 0x85 => Some(PushArray)
        case 0x86 => Some(PushNull)
        case 0x87 => Some(PushUndefined)
        
        case 0x90 => Some(PushGroup(parameter))
        case 0x91 => Some(PushKey(parameter))
        
        case _ => None
      }
      
      instruction
    }
    
    instr match {
      case Some(i) => readStream(buffer, table, acc :+ i)
      case None => acc
    }
  }
  
  private def readLineInfo(buffer: ByteBuffer): Option[(Int, Int, String)] = {
    try {
      val line = buffer.getInt
      val col = buffer.getInt
      readString(buffer) map { str => (line, col, str) }
    } catch {
      case _ => None
    }
  }
  
  private def readString(buffer: ByteBuffer): Option[String] = {
    try {
      val back = Some(buffer.asCharBuffer.toString)
      buffer.rewind()
      back
    } catch {
      case _ => None
    }
  }
  
  private def readNum(buffer: ByteBuffer): Option[String] = readString(buffer)
  
  private def readInt(buffer: ByteBuffer): Option[Int] = {
    try {
      val back = Some(buffer.getInt)
      buffer.rewind()
      back
    } catch {
      case _ => None
    }
  }
  
  private def readSymbolTable(buffer: ByteBuffer): Map[Int, ByteBuffer] = {
    def loop(acc: Map[Int, ByteBuffer]): Map[Int, ByteBuffer] = {
      val id = buffer.getInt()
      if (id == 0) {
        acc
      } else {
        val len = buffer.getInt()
        loop(acc + (id -> getBuffer(buffer, len)))
      }
    }
    
    loop(Map())
  }
  
  private def getBuffer(buffer: ByteBuffer, len: Int): ByteBuffer = {
    val back = ByteBuffer.allocate(len)
    val arr = new Array[Byte](len)
    buffer.get(arr)
    back.put(arr)
    back.rewind()
    back
  }
}
