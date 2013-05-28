package com.precog
package yggdrasil

import blueeyes.json._

import org.objectweb.howl.log._

import java.nio.ByteBuffer
import java.io.{ File, FileOutputStream }
import java.util.Arrays

import com.weiglewilczek.slf4s.Logging

object PersistentJValue {
  sealed abstract class Message(val Flag: Byte) {
    def apply(bytes0: Array[Byte]): Array[Byte] = {
      val bytes = new Array[Byte](bytes0.length + 1)
      ByteBuffer.wrap(bytes).put(bytes0).put(Flag)
      bytes
    }

    def unapply(bytes: Array[Byte]): Option[Array[Byte]] = {
      if (bytes.length > 0 && bytes(bytes.length - 1) == Flag) {
        Some(Arrays.copyOf(bytes, bytes.length - 1))
      } else {
        None
      }
    }
  }

  object Update extends Message(1: Byte)
  object Written extends Message(2: Byte)

  private def open(baseDir: File, fileName: String): Logger = {
    val config = new Configuration()
    config.setLogFileDir(baseDir.getCanonicalPath)
    config.setLogFileName(fileName)
    config.setLogFileExt("log")
    config.setLogFileMode("rwd")
    config.setChecksumEnabled(true)
    val log = new Logger(config)
    log.open()
    log
  }
}

final case class PersistentJValue(baseDir: File, fileName: String)
extends Logging {
  import PersistentJValue._

  private val log = open(baseDir, fileName)
  private val file: File = new File(baseDir, fileName)
  private var jv: JValue = JUndefined

  replay()

  /** Returns the persisted `JValue`. */
  def json: JValue = jv

  /** Updates and persists (blocking) the `JValue`. */
  def json_=(value: JValue) { jv = value; flush() }

  def close() = log.close()

  private def flush() {
    val rawJson = jv.renderCompact.getBytes("UTF-8")
    val mark = log.put(Update(rawJson), true)

    val out = new FileOutputStream(file)
    out.write(rawJson)
    out.close()
    log.put(Written(fileName.getBytes("UTF-8")), true)

    log.mark(mark, true)
  }

  private def replay() {
    var pending: Option[Array[Byte]] = None
    var lastUpdate: Option[Array[Byte]] = None

    log.replay(new ReplayListener {
      def getLogRecord: LogRecord = new LogRecord(1024 * 64)
      def onError(ex: LogException): Unit = throw ex
      def onRecord(rec: LogRecord): Unit = rec.`type` match {
        case LogRecordType.END_OF_LOG =>
          logger.debug("Versions TX log replay complete in " + baseDir.getCanonicalPath)

        case LogRecordType.USER =>
          val bytes = rec.getFields()(0)
          bytes match {
            case Update(rawJson) =>
              pending = Some(rawJson)
            case Written(rawPath) =>
              lastUpdate = Some(rawPath)
              pending = None
            case _ =>
              sys.error("Found unknown user record!")
          }

        case other =>
          logger.warn("Unknown LogRecord type: " + other)
      }
    })

    (pending, lastUpdate) match {
      case (None, Some(_)) =>
        jv = JParser.parseFromFile(file).valueOr(throw _)

      case (Some(rawJson), _) =>
        jv = JParser.parseFromString(new String(rawJson, "UTF-8")).valueOr(throw _)
        flush()

      case (None, None) =>
        flush()
    }
  }
}
