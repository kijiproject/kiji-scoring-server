/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.scoring.server

import java.io.File
import java.io.PrintWriter

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.eclipse.jetty.deploy.DeploymentManager
import org.eclipse.jetty.deploy.providers.WebAppProvider
import org.eclipse.jetty.overlays.OverlayedAppProvider
import org.eclipse.jetty.server.AbstractConnector
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.handler.ContextHandlerCollection
import org.eclipse.jetty.server.handler.DefaultHandler
import org.eclipse.jetty.server.handler.HandlerCollection

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.flow.util.ResourceUtil.doAndClose
import org.kiji.modelrepo.KijiModelRepository
import org.kiji.schema.Kiji
import org.kiji.schema.KijiURI

/**
 * Configuration parameters for a Kiji ScoringServer.
 *
 * @param port on which the configured ScoringServer will listen.
 * @param repo_uri string of the Kiji instance in which the model repository this server will read
 *     from resides.
 * @param repo_scan_interval in seconds between scans of the model repository table. 0 indicates no
 *     automatic scanning.
 * @param num_acceptors threads to be run by the configured server.
 */
@ApiAudience.Public
@ApiStability.Experimental
final case class ServerConfiguration(
  port: Int,
  repo_uri: String,
  repo_scan_interval: Int,
  num_acceptors: Int
)

/**
 * Wraps a Jetty server to provide remote scoring capabilities.
 *
 * @param baseDir in which the models and conf directories exist.
 * @param serverConfig containing information such as the URI of the Kiji instance whose model repo
 *     table will back this ScoringServer.
 */
@ApiAudience.Public
@ApiStability.Experimental
final class ScoringServer private(baseDir: File, serverConfig: ServerConfiguration) {

  val modelRepoURI: KijiURI = KijiURI.newBuilder(serverConfig.repo_uri).build()

  // Build the web.xml that will define the server deployment.
  val servletConfigs: Set[String] = Set(
      Templates.generateServletConfigForWebXml(
          name = "ModelRepoScannerServlet",
          servletClass = "org.kiji.scoring.server.servlets.ModelRepoScannerServlet",
          urlPattern = "/scanner",
          initParams = Map(
              "base-dir" -> baseDir.getAbsolutePath,
              "scan-interval-seconds" -> serverConfig.repo_scan_interval.toString,
              "repo-uri" -> serverConfig.repo_uri
          )
      ), Templates.generateServletConfigForWebXml(
          name = "ListModelsServlet",
          servletClass = "org.kiji.scoring.server.servlets.ListModelsServlet",
          urlPattern = "/list",
          initParams = Map()
      ), Templates.generateServletConfigForWebXml(
          name = "GetModelServlet",
          servletClass = "org.kiji.scoring.server.servlets.GetModelServlet",
          urlPattern = "/get",
          initParams = Map()
      ), Templates.generateServletConfigForWebXml(
          name = "PingServlet",
          servletClass = "org.kiji.scoring.server.servlets.PingServlet",
          urlPattern = "/ping",
          initParams = Map()
      )
  )
  val webXml: String = Templates.generateWebXml(servletConfigs)
  val webXmlDir: File = new File(baseDir, "server/webapps/admin/WEB-INF")
  webXmlDir.mkdirs()
  val webXmlFile: File = new File(webXmlDir, "web.xml")
  webXmlFile.createNewFile()
  doAndClose(new PrintWriter(webXmlFile, "UTF-8")) {
    pw => pw.print(webXml)
  }

  val server: Server = new Server(serverConfig.port)
  // Increase the number of acceptor threads.
  val connector: AbstractConnector = server.getConnectors()(0).asInstanceOf[AbstractConnector]
  connector.setAcceptors(serverConfig.num_acceptors)
  val handlers: HandlerCollection = new HandlerCollection()
  val contextHandler: ContextHandlerCollection = new ContextHandlerCollection()
  val deploymentManager: DeploymentManager = new DeploymentManager()
  val overlayedProvider: OverlayedAppProvider = new OverlayedAppProvider
  val webappDeployer: WebAppProvider = new WebAppProvider()
  webappDeployer.setMonitoredDirName(new File(baseDir, "server/webapps").getAbsolutePath)
  overlayedProvider.setScanDir(new File(baseDir, ScoringServer.MODELS_FOLDER))
  // For now scan this directory once per second.
  overlayedProvider.setScanInterval(1)
  deploymentManager.setContexts(contextHandler)
  deploymentManager.addAppProvider(overlayedProvider)
  deploymentManager.addAppProvider(webappDeployer)
  handlers.addHandler(contextHandler)
  handlers.addHandler(new DefaultHandler())
  server.setHandler(handlers)
  server.addBean(deploymentManager)

  /** Start the ScoringServer */
  def start() {
    server.start()
  }

  /** Stop the ScoringServer */
  def stop() {
    server.stop()
  }
}

/**
 * Main entry point for the scoring server. This pulls in and combines various Jetty components
 * to boot a new web server listening for scoring requests.
 */
@ApiAudience.Public
@ApiStability.Experimental
object ScoringServer {

  val CONF_FILE: String = "configuration.json"
  val MODELS_FOLDER: String = "models"
  val LOGS_FOLDER: String = "logs"
  val CONF_FOLDER: String = "conf"

  def main(args: Array[String]): Unit = {

    // Check that we started in the right location else bomb out
    val missingFiles: Set[String] = checkIfStartedInProperLocation()
    if (!(missingFiles.size == 0)) {
      sys.error("Missing files: %s".format(missingFiles.mkString(", ")))
    }

    // TODO what does this null do? should it be args(0)?
    val scoringServer = ScoringServer(null)

    scoringServer.start()
    scoringServer.server.join()
  }

  /**
   * Constructs a ScoringServer instance configured using the conf/configuration.json found in the
   * specified base directory.
   *
   * @param baseDir in which models and configuration directories are expected to exist.
   * @return a new ScoringServer.
   */
  def apply(baseDir: File): ScoringServer = {
    new ScoringServer(baseDir, getConfig(new File(baseDir, "%s/%s".format(CONF_FOLDER, CONF_FILE))))
  }

  /**
   * Constructs a ScoringServer instance configured with the given ServerConfiguration.
   *
   * @param baseDir in which the models directory is expected to exist.
   * @return a new ScoringServer.
   */
  def apply(baseDir: File, conf: ServerConfiguration): ScoringServer = {
    new ScoringServer(baseDir, conf)
  }

  /**
   * Checks that the server is started in the right location by ensuring the presence of a few key
   * directories under the conf, models and logs folder.
   *
   * @return whether or not the key set of folders exist or not.
   */
  def checkIfStartedInProperLocation(): Set[String] = {
    // Get the list of required files which do not exist.
    Set(
        CONF_FOLDER + "/" + CONF_FILE,
        MODELS_FOLDER + "/webapps",
        MODELS_FOLDER + "/instances",
        MODELS_FOLDER + "/templates",
        LOGS_FOLDER
    ).filter {
      (fileString: String) => !new File(fileString).exists()
    }
  }

  /**
   * Returns the ServerConfiguration object constructed from conf/configuration.json.
   *
   * @param confFile is the location of the configuration used to configure the server.
   * @return the ServerConfiguration object constructed from conf/configuration.json.
   */
  def getConfig(confFile: File): ServerConfiguration = {
    val configMapper = new ObjectMapper
    configMapper.registerModule(DefaultScalaModule)
    configMapper.readValue(confFile, classOf[ServerConfiguration])
  }
}
