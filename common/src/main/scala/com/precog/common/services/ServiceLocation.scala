package com.precog.common
package services

import blueeyes.core.http.URI

import org.streum.configrity.Configuration

import scalaz._
import scalaz.NonEmptyList._
import scalaz.syntax.applicative._
import scalaz.syntax.std.option._

case class ServiceLocation(protocol: String, host: String, port: Int, pathPrefix: Option[String]) {
  def toURI: URI = URI(scheme = Some(protocol), host = Some(host), port = Some(port), path = pathPrefix)
}

object ServiceLocation {
  def fromConfig(conf: Configuration): ValidationNel[String, ServiceLocation] = {
    (conf.get[String]("protocol").toSuccess(nels("Configuration property protocol is required")) |@|
     conf.get[String]("host").toSuccess(nels("Configuration property host is required")) |@|
     conf.get[Int]("port").toSuccess(nels("Configuration property port is required"))) { (protocol, host, port) =>
      ServiceLocation(protocol, host, port, conf.get[String]("pathPrefix"))  
    } 
  }
}




