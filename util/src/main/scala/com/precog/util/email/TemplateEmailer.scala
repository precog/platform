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
package com.precog.util
package email

import java.io.File
import java.util.Properties

import javax.mail._
import javax.mail.internet._

import org.fusesource.scalate._

/**
  * Simplifies sending emails using file templates. Mail properties
  * are either provided or taken from System properties (set
  * via -D), with defaults listed below if none are found. See
  * http://javamail.kenai.com/nonav/javadocs/overview-summary.html for
  * details. Most commonly used properties are
  *
  * $ - mail.smtp.host - the hostname of the mail server to use (default: localhost)
  * $ - mail.smtp.port - the port of the mail server to use (default: 25)
  * $ - mail.from - the from address for sent emails (no default)
  *
  * @param defaultParameters The default parameters used for the templates,
  *        in addition to any user-supplied params in the call to sendEmail
  * @param javaMailProps - Additional javamail properties to use
  * @param workDir - An optional temp directory for scalate to work in
  */
abstract class TemplateEmailer(defaultParameters: Map[String, String], javaMailProps: Option[Properties], workDir: Option[File]) {
  val engine = new TemplateEngine
  workDir.foreach { dir => engine.workingDirectory = dir }

  private val mailProps = javaMailProps.getOrElse {
    val props = new Properties
    val sysProps = System.getProperties

    props.setProperty("mail.smtp.host", Option(sysProps.getProperty("mail.smtp.host")).getOrElse("localhost"))
    props.setProperty("mail.smtp.port", Option(sysProps.getProperty("mail.smtp.host")).getOrElse("25"))
    props
  }

  def withSession[T](f: Session => T): T = {
    f(Session.getInstance(mailProps))
  }

  protected def source(name: String): TemplateSource

  /**
    * Send an email to the provided recipients processing the subject and body templates (looked up by name) via scalate. The body templates are
    * specified as (template name, mime type) pairs. All templates and parameters are assumed to be in UTF8.
    */
  def sendEmail(recipients: Seq[String], subjectTemplate: String, bodyTemplates: Seq[(String, String)], parameters: Map[String, String], from: Option[String] = None): PrecogUnit = withSession { session =>
    val msg = new MimeMessage(session)
    msg.addRecipients(Message.RecipientType.TO, recipients.map(new InternetAddress(_).asInstanceOf[Address]).toArray)
    from.foreach { fa => msg.setFrom(new InternetAddress(fa)) }

    val templateParams = defaultParameters ++ parameters

    // Process the templates and insert values
    msg.setSubject(engine.layout(source(subjectTemplate), templateParams))

    val transformed = bodyTemplates.map {
      case (template, mimetype) =>
        (engine.layout(source(template), templateParams), mimetype + "; charset=utf-8")
    }

    if (transformed.size > 1) {
      val multi = new MimeMultipart("alternative")
      transformed.foreach {
        case (content, mimetype) =>
          val bodyPart = new MimeBodyPart
          bodyPart.setContent(content, mimetype)
          multi.addBodyPart(bodyPart)
      }
      msg.setContent(multi)
    } else {
      transformed.foreach {
        case (content, mimetype) => msg.setContent(content, mimetype)
      }
    }

    Transport.send(msg)

    PrecogUnit
  }
}

class DirectoryTemplateEmailer(templateDir: File, defaultParameters: Map[String, String], javaMailProps: Option[Properties] = None, workDir: Option[File] = None) extends TemplateEmailer(defaultParameters, javaMailProps, workDir) {
  protected def source(name: String) = TemplateSource.fromFile(new File(templateDir, name))
}

class ClassLoaderTemplateEmailer(defaultParameters: Map[String, String], javaMailProps: Option[Properties] = None, workDir: Option[File] = None) extends TemplateEmailer(defaultParameters, javaMailProps, workDir) {
  protected def source(name: String) = TemplateSource.fromURL(this.getClass.getClassLoader.getResource(name))
}
