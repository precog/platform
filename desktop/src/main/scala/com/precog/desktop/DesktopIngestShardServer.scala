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
package com.precog.shard
package desktop

import akka.actor.ActorSystem
import akka.dispatch.{ExecutionContext, Future, Promise}

import blueeyes.bkka.{FutureMonad, Stoppable}
import blueeyes.core.data.ByteChunk
import blueeyes.core.http._

import com.precog.common.jobs.InMemoryJobManager
import com.precog.common.accounts.StaticAccountFinder
import com.precog.common.security.StaticAPIKeyFinder
import com.precog.shard.nihdb.NIHDBQueryExecutorComponent
import com.precog.standalone._
import com.precog.ingest.{EventServiceDeps, EventService}
import com.precog.ingest.kafka.KafkaEventStore

import java.awt._
import java.awt.event._
import javax.swing._

import java.io.IOException
import java.net.{InetAddress, Socket}
import java.util.Properties

import kafka.server.{KafkaConfig, KafkaServerStartable}

import org.apache.zookeeper.server._

import org.streum.configrity.Configuration

import scala.annotation.tailrec
import scala.collection.JavaConverters._

import scalaz.{EitherT, Monad}

object DesktopIngestShardServer
    extends StandaloneShardServer
    with StandaloneIngestServer
    with NIHDBQueryExecutorComponent {
  val caveatMessage = None

  val actorSystem = ActorSystem("desktopExecutorActorSystem")
  implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
  implicit val M: Monad[Future] = new FutureMonad(executionContext)

  def configureShardState(config: Configuration, rootConfig: Configuration) = Future {
    val guiNotifier = if (rootConfig[Boolean]("appwindow.enabled", false)) {
      logger.info("Starting gui window")
      // Set some useful OS X props (just in case)
      System.setProperty("apple.laf.useScreenMenuBar", "true")
      System.setProperty("apple.awt.graphics.UseQuartz", "true")
      System.setProperty("apple.awt.textantialiasing", "true")
      System.setProperty("apple.awt.antialiasing", "true")
      // Add a window with a menu for shutdown
      val precogMenu = new JMenu("Precog for Desktop")
      val quitItem = new JMenuItem("Quit", KeyEvent.VK_Q)
      quitItem.addActionListener(new ActionListener {
        def actionPerformed(ev: ActionEvent) = {
          System.exit(0)
        }
      })

      precogMenu.add(quitItem)

      val labcoatMenu = new JMenu("Labcoat")
      val launchItem = new JMenuItem("Launch", KeyEvent.VK_L)
      launchItem.addActionListener(new ActionListener {
        def actionPerformed(ev: ActionEvent) = {
          LaunchLabcoat.launchBrowser(rootConfig)
        }
      })

      labcoatMenu.add(launchItem)

      val menubar = new JMenuBar
      menubar.add(precogMenu)
      menubar.add(labcoatMenu)

      val appFrame = new JFrame("Precog for Desktop")
      appFrame.setJMenuBar(menubar)

      appFrame.addWindowListener(new WindowAdapter {
        override def windowClosed(ev: WindowEvent) = {
          System.exit(0)
        }
      })

      val notifyArea = new JTextArea(25, 80)
      notifyArea.setEditable(false)
      appFrame.add(notifyArea)

      appFrame.pack()

      appFrame.setVisible(true)

      Some({ (msg: String) => notifyArea.append(msg + "\n") })
    } else {
      logger.info("Skipping gui window")
      None
    }

    guiNotifier.foreach(_("Starting internal services"))
    val zookeeper = startZookeeperStandalone(rootConfig.detach("zookeeper"))

    logger.debug("Waiting for ZK startup")

    if (!waitForPortOpen(rootConfig[Int]("zookeeper.port"), 60)) {
      throw new Exception("Timeout waiting for ZK port to open")
    }

    val kafka = startKafkaStandalone(rootConfig)

    logger.debug("Waiting for Kafka startup")

    if (!waitForPortOpen(rootConfig[Int]("kafka.port"), 60)) {
      throw new Exception("Time out waiting on kafka port to open")
    }

    guiNotifier.foreach(_("Internal services started, bringing up Precog"))

    println("Configuration at configure shard state=%s".format(config))
    val rootAPIKey = config[String]("security.masterAccount.apiKey")
    val apiKeyFinder = new StaticAPIKeyFinder(rootAPIKey)
    val accountFinder = new StaticAccountFinder(rootAPIKey, "desktop")
    val jobManager = new InMemoryJobManager
    val platform = platformFactory(config.detach("queryExecutor"), apiKeyFinder, accountFinder, jobManager)

    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run() = {
        logger.info("Shutting down embedded kafka instances")
        kafka.shutdown
        kafka.awaitShutdown

        logger.info("Kafka shutdown complete, stopping ZK")
        zookeeper.stop
        logger.info("ZK shutdown complete")
      }
    })

    val stoppable = Stoppable.fromFuture {
      platform.shutdown.onComplete { _ =>
        logger.info("Platform shutdown complete")
      }.onFailure {
        case t: Throwable =>
          logger.error("Failure during platform shutdown", t)
      }
    }

    val state = ManagedQueryShardState(platform, apiKeyFinder, jobManager, clock, ShardStateOptions.NoOptions, stoppable)
    guiNotifier.foreach(_("Precog startup complete"))
    logger.debug("Startup config complete. Returning " + state)
    state
  } recoverWith {
    case ex: Throwable =>
      System.err.println("Could not start NIHDB Shard server!!!")
      ex.printStackTrace
      Promise.failed(ex)
  }

  def waitForPortOpen(port: Int, tries: Int): Boolean = {
    var remainingTries = tries
    while (remainingTries > 0) {
      try {
        val conn = new Socket(InetAddress.getLocalHost, port)
        conn.close()
        return true
      } catch {
        case ioe: IOException =>
          Thread.sleep(500)
          remainingTries -= 1
      }
    }

    logger.error("Port %d did not open in %d tries".format(port, tries))
    false
  }

  def startKafkaStandalone(config: Configuration): KafkaServerStartable = {
    val defaultProps = new Properties
    defaultProps.setProperty("brokerid", "0")
    defaultProps.setProperty("num.threads", "8")
    defaultProps.setProperty("socket.send.buffer", "1048576")
    defaultProps.setProperty("socket.receive.buffer", "1048576")
    defaultProps.setProperty("max.socket.request.bytes", "104857600")
    defaultProps.setProperty("num.partitions", "1")
    defaultProps.setProperty("log.flush.interval", "10000")
    defaultProps.setProperty("log.default.flush.interval.ms", "1000")
    defaultProps.setProperty("log.default.flush.scheduler.interval.ms", "1000")
    defaultProps.setProperty("log.retention.hours", "107374182")
    defaultProps.setProperty("log.file.size", "536870912")
    defaultProps.setProperty("log.cleanup.interval.mins", "1")
    defaultProps.setProperty("zk.connectiontimeout.ms", "1000000")

    defaultProps.setProperty("zk.connect", "localhost:" + config[String]("zookeeper.port"))

    val centralProps = defaultProps.clone.asInstanceOf[Properties]
    centralProps.putAll(config.detach("kafka").data.asJava)
    centralProps.setProperty("enable.zookeeper", "true")

    val central = new KafkaServerStartable(new KafkaConfig(centralProps))

    (new Thread {
      override def run() = {
        central.startup
      }
    }).start()

    central
  }

  class EmbeddedZK extends ZooKeeperServerMain {
    def stop = shutdown()
  }

  def startZookeeperStandalone(config: Configuration): EmbeddedZK = {
    val serverConfig = new ServerConfig
    serverConfig.parse(Array[String](config[String]("port"), config[String]("dataDir")))

    val server = new EmbeddedZK

    (new Thread {
      override def run() = {
        server.runFromConfig(serverConfig)
      }
    }).start()

    server
  }
}

object LaunchLabcoat{
  def main(args:Array[String]){

    if(args.size != 1 || !args(0).startsWith("--configFile=") ){
      sys.error("Wrong parameters. Usage: --configFile=<configuration>")
    } else {
      import java.awt.Desktop

      val configFile=args(0).replaceFirst("--configFile=","")
      val config = Configuration.load( configFile )
      launchBrowser(config)
      System.exit(0)
    }
  }

  def launchBrowser(config: Configuration): Unit = {
    val jettyPort = config[Int]("services.analytics.v2.labcoat.port")
    val shardPort = config[Int]("server.port")
    val zkPort = config[Int]("zookeeper.port")
    val kafkaPort = config[Int]("kafka.port")

    launchBrowser(jettyPort, shardPort, zkPort, kafkaPort)
  }

  def launchBrowser(jettyPort: Int, shardPort: Int, zkPort: Int, kafkaPort: Int): Unit = {
    def waitForPorts=
      DesktopIngestShardServer.waitForPortOpen(jettyPort, 15) &&
        DesktopIngestShardServer.waitForPortOpen(shardPort, 15) &&
        DesktopIngestShardServer.waitForPortOpen(zkPort, 15) &&
        DesktopIngestShardServer.waitForPortOpen(kafkaPort, 15)

    @tailrec
    def doLaunch(){
      if (waitForPorts) {
        java.awt.Desktop.getDesktop.browse(new java.net.URI("http://localhost:%s".format(jettyPort)))
      } else {
        import javax.swing.JOptionPane
        JOptionPane.showMessageDialog(null,
          "Waiting for server to start.\nPlease check that PrecogService has been launched\nRetrying...",
          "Labcoat launcher", JOptionPane.WARNING_MESSAGE)
        doLaunch()
      }
    }

    if (Desktop.isDesktopSupported){
      doLaunch()
    } else {
      sys.error("Browser open on non-desktop system")
    }
  }
}
