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

package org.kiji.scoring.server.servlets

import java.io.File
import java.io.PrintWriter
import java.util.{ Map => JMap }
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.mutable
import scala.io.BufferedSource
import scala.io.Source

import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.eclipse.jetty.overlays.OverlayedAppProvider
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.flow.util.ResourceUtil.doAndClose
import org.kiji.express.flow.util.ResourceUtil.withKiji
import org.kiji.modelrepo.ArtifactName
import org.kiji.modelrepo.KijiModelRepository
import org.kiji.modelrepo.ModelContainer
import org.kiji.modelrepo.uploader.MavenArtifactName
import org.kiji.schema.KijiURI
import org.kiji.scoring.server.ScoringServer
import org.kiji.scoring.server.Templates

/**
 * GET to immediately scan the model repository. Returns a list of models deployed and undeployed.
 * POST ?do=deploy&model=fully.qualified.model.name-1.0.0 to manually deploy a model.
 * POST ?do=undeploy&model=fully.qualified.model.name-1.0.0 to manually undeploy a model. Models
 *     which are marked as "Production Ready" in the model repository will be redeployed on the next
 *     scan of the model repository.
 *
 * Manages deploying and undeploying models from the model repository.
 */
@ApiAudience.Public
@ApiStability.Experimental
class ModelRepoScannerServlet extends HttpServlet with Runnable {
  import ModelRepoScannerServlet._

  private var baseDir: File = null
  private var scanIntervalSeconds: Int = null.asInstanceOf[Int]
  private var kijiModelRepo: KijiModelRepository = null
  private var webappsFolder: File = null
  private var instancesFolder: File = null
  private var templatesFolder: File = null
  private var artifactToInstanceDir: mutable.Map[ArtifactName, File] = null
  private var deployedWarFiles: mutable.Map[String, String] = null
  private val manuallyDeployedModels: mutable.Set[ArtifactName] = mutable.Set()
  private[this] var state: ModelRepoScannerState = Initializing

  private val README_FILE = "README.txt"

  override def init() {
    baseDir = new File(getInitParameter("base-dir"))
    scanIntervalSeconds = getInitParameter("scan-interval-seconds").toInt
    kijiModelRepo = getKijiModelRepo(getInitParameter("repo-uri"))

    /** The webapps folder relative to the base directory of the server.server. */
    webappsFolder = new File(
      baseDir, new File(ScoringServer.MODELS_FOLDER, OverlayedAppProvider.WEBAPPS).getPath)

    /** The instances folder relative to the base directory of the server. */
    instancesFolder = new File(
      baseDir, new File(ScoringServer.MODELS_FOLDER, OverlayedAppProvider.INSTANCES).getPath)

    /** The templates folder relative to the base directory of the server. */
    templatesFolder = new File(
      baseDir, new File(ScoringServer.MODELS_FOLDER, OverlayedAppProvider.TEMPLATES).getPath)

    /** Mapping of deployed models to the physical location of the deployment. */
    artifactToInstanceDir = mutable.Map[ArtifactName, File]()
    // Expose the set of deployed artifacts to the servlet context so that List and Get model
    // servlets can see them.
    getServletContext.setAttribute(ModelsListContextKey, artifactToInstanceDir)

    /**
     *  Stores a map of model artifact locations to their corresponding template name so that
     *  instances can be properly mapped when created against a war file that has already
     *  been previously deployed. The key is the location string from the location column
     *  in the model repo and the value is the fully qualified name of the model
     *  (group.artifact-version) to which this location is mapped to.
     */
    deployedWarFiles = mutable.Map[String, String]()

    // This will load up the in memory data structures for the scoring server
    // based on information from disk. First go through the webapps and any webapp that doesn't
    // have a corresponding location file, delete.

    // Validate webapps
    val (_, invalidWarFiles) = webappsFolder.listFiles.partition {
      file: File =>
        file.isFile &&
          ((file.getName.endsWith(".war") &&
            new File(webappsFolder, file.getName + ".loc").exists) ||
            (file.getName.endsWith(".loc") &&
              new File(webappsFolder,
                file.getName.dropRight(4)).exists()) || file.getName() == README_FILE)
    }
    invalidWarFiles.foreach { delete }

    // Validate the templates to make sure that they are pointing to a valid
    // war file.
    val (validTemplates, invalidTemplates) = templatesFolder.listFiles.partition {
      templateDir: File =>
        {
          val (_, warBaseName) = parseDirectoryName(templateDir.getName)
          // For a template to be valid, it must have a name and warBaseName AND the
          // warBaseName.war must exist AND warBaseName.war.loc must also exist
          val warFile = new File(webappsFolder, warBaseName.getOrElse("") + ".war")
          val locFile = new File(webappsFolder, warBaseName.getOrElse("") + ".war.loc")

          templateDir.getName() == README_FILE ||
            (templateDir.isDirectory && !warBaseName.isEmpty && warFile.exists && locFile.exists)
        }
    }

    invalidTemplates.foreach {
      template: File => {
          LOG.info("Deleting invalid template directory {}", template)
          delete(template)
        }
    }

    validTemplates.foreach {
      template: File => {
          if (template.isDirectory()) {
            val (templateName, warBaseName) = parseDirectoryName(template.getName)
            val locFile = new File(webappsFolder, warBaseName.get + ".war.loc")
            val location = getLocationInformation(locFile)
            deployedWarFiles.put(location, templateName)
          }
        }
    }

    // Loop through the instances and add them to the map.
    instancesFolder.listFiles.foreach {
      instance: File => {
          if (instance.getName() != README_FILE) {
            //templateName=artifactFullyQualifiedName
            val (templateName, artifactName) = parseDirectoryName(instance.getName)

            // This is an inefficient lookup on validTemplates but it's a one time thing on
            // startup of the server scanner.
            if (!instance.isDirectory ||
              artifactName.isEmpty ||
              !validTemplates.contains(new File(templatesFolder, templateName))) {
              LOG.info("Deleting invalid instance " + instance.getPath)
              delete(instance)
            } else {
              try {
                val parsedArtifact = new ArtifactName(artifactName.get)
                artifactToInstanceDir.put(parsedArtifact, instance)
              } catch {
                // Indicates an invalid ArtifactName.
                case ex: IllegalArgumentException => delete(instance)
              }
            }
          }
        }
    }

    if (scanIntervalSeconds > 0) { checkForUpdates() }

    state = Running

    val thread = new Thread(this)
    thread.start
  }

  override def destroy() {
    shutdown
    kijiModelRepo.close()
  }

  override def doGet(
    request: HttpServletRequest,
    response: HttpServletResponse) {
    val (deployed, undeployed) = checkForUpdates()
    response.getWriter.print(
      """{
          |  "deployed":"%s",
          |  "undeployed":"%s"
          |}
        """.stripMargin.format(
        Templates.iterableToJson(deployed), Templates.iterableToJson(undeployed)))
  }

  override def doPost(
    request: HttpServletRequest,
    response: HttpServletResponse) {
    val operation = request.getParameter("do")
    val model = new ArtifactName(request.getParameter("model"))
    manuallyDeployedModels.synchronized {
      operation match {
        case "deploy" => {
          LOG.info("Deploying model: {}", model.getFullyQualifiedName)
          deployArtifact(kijiModelRepo.getModelContainer(model))
          response.setStatus(200)
          response.getWriter.print("model: %s deployed".format(model))
        }
        case "undeploy" => {
          // TODO add a warning about undeploying a model that will just get added by the next scan
          LOG.info("Undeploying model: {}", model.getFullyQualifiedName)
          manuallyDeployedModels.remove(model)
          delete(artifactToInstanceDir(model))
          response.setStatus(200)
          response.getWriter.print("model: %s undeployed".format(model))
        }
        case unknown => response.sendError(400, "unknown 'do' operation: %s".format(unknown))
      }
    }
  }

  /**
   * Turns off the internal run flag to safely stop the scanning of the model repository
   * table.
   */
  def shutdown() { state = Stopped }

  override def run() {
    if (scanIntervalSeconds > 0) {
      while (state == Running) {
        LOG.debug("Scanning model repository for changes...")
        val (deployed, undeployed) = checkForUpdates()
        LOG.debug("New models deployed: {}. Old models undeployed: {}.{}", deployed, undeployed, "")
        Thread.sleep(scanIntervalSeconds * 1000L)
      }
    } else {
      LOG.debug(
        "ModelRepoScannerServlet started without scanning interval; Will not automatically scan.")
    }
  }

  /**
   * Checks the model repository table for updates and deploys/undeploys models as necessary.
   *
   * @return (newly deployed models, undeployed models)
   */
  def checkForUpdates(): (Set[ArtifactName], Set[ArtifactName]) = {
    val allEnabledModels: Map[ArtifactName, ModelContainer] = getAllEnabledModels.
      foldLeft(Map[ArtifactName, ModelContainer]()) {
        (accumulatorMap: Map[ArtifactName, ModelContainer], model: ModelContainer) =>
          accumulatorMap + (model.getArtifactName -> model)
      }

    // Split the model map into those that are already deployed and those that should be undeployed
    // based on whether or not the currently enabled models contain the deployed model.
    val (toKeep, toUndeploy) = artifactToInstanceDir.partition {
      kv: (ArtifactName, File) =>
        allEnabledModels.contains(kv._1) || manuallyDeployedModels.contains(kv._1)
    }

    // For each model to undeploy, remove it.
    toUndeploy.foreach {
      kv: (ArtifactName, File) =>
        {
          val (artifactName, location) = kv
          LOG.info("Undeploying model: %s from location: %s".format(
            artifactName.getFullyQualifiedName, location))
          delete(location)
        }
    }

    // Now find the set of lifecycles to add by diffing the current with the already deployed and
    // add those.
    val toDeploy: Set[ArtifactName] = allEnabledModels.keySet.diff(toKeep.keySet)
    toDeploy.foreach {
      artifactName: ArtifactName =>
        {
          val model: ModelContainer = allEnabledModels(artifactName)
          LOG.info("Deploying model: " + artifactName.getFullyQualifiedName)
          deployArtifact(model)
        }
    }

    (toDeploy, toUndeploy.keySet.toSet)
  }

  /**
   * Deploys the specified model artifact by either creating a new Jetty instance or by
   * downloading the artifact and setting up a new template/instance in Jetty.
   *
   * @param model is the specified ModelContainer to deploy.
   */
  private def deployArtifact(
    model: ModelContainer) {
    val mavenArtifact: MavenArtifactName = new MavenArtifactName(model.getArtifactName)
    val artifact: ArtifactName = model.getArtifactName

    val fullyQualifiedName: String = artifact.getFullyQualifiedName

    val contextPath = contextPathFromArtifact(artifact)

    // Populate a map of the various placeholder values to substitute in files
    val templateParamValues: Map[String, String] = Map(
      MODEL_NAME -> artifact.getFullyQualifiedName,
      CONTEXT_PATH -> contextPath,
      GenericScoringServlet.ATTACHED_COLUMN_KEY ->
        model.getModelContainer.getColumnName,
      GenericScoringServlet.SCORE_FUNCTION_CLASS_KEY ->
        model.getModelContainer.getScoreFunctionClass,
      GenericScoringServlet.TABLE_URI_KEY ->
        model.getModelContainer.getTableUri,
      GenericScoringServlet.RECORD_PARAMETERS_KEY ->
        GenericScoringServlet.GSON.toJson(
          model.getModelContainer.getParameters, classOf[JMap[String, String]]))

    deployedWarFiles.get(model.getLocation) match {
      case Some(templateName: String) => {
        // The war file for this model already exists, deploy a new instance with the same war.
        createNewInstance(model, templateName, templateParamValues)
      }
      case None => {
        // The war file for this model is not currently deployed.
        // Download the artifact to a temporary location.
        val artifactFile: File = File.createTempFile("artifact", "war")
        val finalArtifactName: String = "%s.%s".format(
          mavenArtifact.getGroupName, FilenameUtils.getName(model.getLocation))
        model.downloadArtifact(artifactFile)

        // Create a new Jetty template to map to the war file.
        // Template is (fullyQualifiedName=warFileBase)
        val templateDirName: String = "%s=%s".format(
          fullyQualifiedName, FilenameUtils.getBaseName(finalArtifactName))
        val tempTemplateDir: File = new File(Files.createTempDir(), "WEB-INF")
        tempTemplateDir.mkdirs()

        createTemplateXml(new File(tempTemplateDir, "template.xml"))

        // Move the temporarily downloaded artifact to its final location.
        FileUtils.moveFile(artifactFile, new File(webappsFolder, finalArtifactName))

        // As part of the state necessary to reconstruct the scoring server on cold start, write
        // out the location of the model (which is used to determine if a war file needs to
        // actually be deployed when a model is deployed) to a text file.
        writeLocationInformation(finalArtifactName, model)

        FileUtils.moveDirectory(
          tempTemplateDir.getParentFile,
          new File(templatesFolder, templateDirName))

        // Create a new instance.
        createNewInstance(model, fullyQualifiedName, templateParamValues)

        deployedWarFiles.put(model.getLocation, fullyQualifiedName)
      }
    }
  }

  /**
   * Writes out the relative URI of the model (in the model repository) to a known
   * text file in the webapps folder. This is used later on server reboot to make sure that
   * currently enabled and previously deployed models are represented in the internal
   * server state. The file name will be <artifactFileName>.loc.
   *
   * @param artifactFileName is the name of the artifact file name that is being locally deployed.
   * @param model is the model object that is associated with the artifact.
   */
  private def writeLocationInformation(
    artifactFileName: String,
    model: ModelContainer) {
    doAndClose(new PrintWriter(new File(webappsFolder, artifactFileName + ".loc"), "UTF-8")) {
      pw: PrintWriter => pw.println(model.getLocation)
    }
  }

  /**
   * Creates a new Jetty overlay instance.
   *
   * @param model is the ModelContainer to deploy.
   * @param templateName is the name of the template to which this instance belongs.
   * @param bookmarkParams contains a map of parameters and values used when configuring the WEB-INF
   *     specific files. An example of a parameter includes the context name used when addressing
   *     this model via HTTP which is dynamically populated based on the fully qualified name of
   *     the ModelContainer.
   */
  private def createNewInstance(
    model: ModelContainer,
    templateName: String,
    bookmarkParams: Map[String, String]) {
    // This will create a new instance by leveraging the template files on the classpath
    // and create the right directory. Maybe first create the directory in a temp location and
    // move to the right place.
    val artifactName: ArtifactName = model.getArtifactName

    val tempInstanceDir: File = new File(Files.createTempDir(), "WEB-INF")
    tempInstanceDir.mkdir()

    // templateName=artifactFullyQualifiedName
    val instanceDirName: String = "%s=%s".format(templateName, artifactName.getFullyQualifiedName)

    createOverlayXml(new File(tempInstanceDir, "overlay.xml"), bookmarkParams(CONTEXT_PATH))
    createWebOverlayXml(new File(tempInstanceDir, "web-overlay.xml"), bookmarkParams - CONTEXT_PATH)

    val finalInstanceDir: File = new File(instancesFolder, instanceDirName)

    FileUtils.moveDirectory(tempInstanceDir.getParentFile, finalInstanceDir)

    artifactToInstanceDir.put(artifactName, finalInstanceDir)
  }

  /**
   * Returns all the currently enabled models from the model repository.
   *
   * @return all the currently enabled models from the model repository.
   */
  private def getAllEnabledModels: Set[ModelContainer] = {
    kijiModelRepo.getModelContainers(null, 1, true).asScala.toSet
  }
}

@ApiAudience.Public
@ApiStability.Experimental
object ModelRepoScannerServlet {
  val LOG: Logger = LoggerFactory.getLogger(classOf[ModelRepoScannerServlet])

  val ModelsListContextKey: String =
    "org.kiji.scoring.server.servlets.ModelRepoScannerServlet.models"

  // Constants that will get used when generating the various files to deploy a model.
  val CONTEXT_PATH: String = "context-path"
  val MODEL_NAME: String = "model-name"

  /** All possible states of the ModelRepoScannerServlet. */
  sealed trait ModelRepoScannerState
  case object Initializing extends ModelRepoScannerState
  case object Running extends ModelRepoScannerState
  case object Stopped extends ModelRepoScannerState

  /**
   * Get the relative URL for a model from its artifact name.
   *
   * @param artifact name of the model for which to get the relative URL.
   * @return the relative URL for the mode.
   */
  def contextPathFromArtifact(artifact: ArtifactName): String = {
    val mavenArtifact: MavenArtifactName = new MavenArtifactName(artifact)
    "models/%s/%s".format(
      "%s/%s".format(mavenArtifact.getGroupName, mavenArtifact.getArtifactName).replace('.', '/'),
      artifact.getVersion.toString)
  }

  /**
   * Create a template.xml file at the target File location.
   *
   * Uses [[org.kiji.scoring.server.Templates.generateTemplateXml()]] to create the contents of the
   * output file.
   *
   * @param targetFile into which to write template.xml contents.
   */
  private def createTemplateXml(
    targetFile: File) {
    doAndClose(new PrintWriter(targetFile, "UTF-8")) {
      pw: PrintWriter => pw.print(Templates.generateTemplateXml())
    }
  }

  /**
   * Create an overlay.xml file at the target File location.
   *
   * Uses [[org.kiji.scoring.server.Templates.generateOverlayXml()]] to format the context path into
   * the output file.
   *
   * @param targetFile into which to write overlay.xml contents.
   */
  private def createOverlayXml(
    targetFile: File,
    contextPath: String) {
    doAndClose(new PrintWriter(targetFile, "UTF-8")) {
      pw: PrintWriter => pw.print(Templates.generateOverlayXml(contextPath))
    }
  }

  /**
   * Create a web-overlay.xml file at the target File location.
   *
   * Uses [[org.kiji.scoring.server.Templates.generateWebOverlayXml()]] to format initParams into
   * the output file.
   *
   * @param targetFile into which to write web-overlay.xml contents.
   */
  private def createWebOverlayXml(
    targetFile: File,
    initParams: Map[String, String]) {
    doAndClose(new PrintWriter(targetFile, "UTF-8")) {
      pw: PrintWriter =>
        {
          val webOverlay = Templates.generateWebOverlayXml(
            initParams(MODEL_NAME), initParams)
          pw.print(webOverlay)
        }
    }
  }

  /**
   * Returns the location information from the specified file. The input file is the name of the
   * location file that was written using the writeLocationInformation method.
   *
   * @param locationFile is the name of the location file containing the model location
   *     information.
   * @return the location information in the file.
   */
  private def getLocationInformation(
    locationFile: File): String = {
    doAndClose(Source.fromFile(locationFile)) {
      inputSource: BufferedSource => inputSource.mkString.trim
    }
  }

  /**
   * Deletes a given file or directory.
   *
   * @param file or directory to delete.
   */
  private def delete(file: File) {
    if (file.isDirectory) {
      FileUtils.deleteDirectory(file)
    } else {
      file.delete()
    }
  }

  /**
   * Parses a Jetty template/instance directory using "=" as the delimiter. Returns the two parts
   * that comprise the directory.
   *
   * @param inputDirectory name to parse.
   * @return a 2-tuple containing the two strings on either side of the "=" operator. If there is no
   *     "=" in the string, then the second part will be None.
   */
  private def parseDirectoryName(
    inputDirectory: String): (String, Option[String]) = {
    val parts: Array[String] = inputDirectory.split("=")
    if (parts.length == 1) {
      (parts(0), None)
    } else if (parts.length == 2) {
      (parts(0), Some(parts(1)))
    } else {
      throw new IllegalArgumentException(
        "Jetty template/instance directory must contain 0 or 1 '=', got: %s".format(inputDirectory))
    }
  }

  /**
   * Get a [[org.kiji.modelrepo.KijiModelRepository]] instance from a KijiURI string.
   *
   * @param repoUri of the instance for which to get a KijiModelRepository.
   * @return a [[org.kiji.modelrepo.KijiModelRepository]] instance.
   */
  def getKijiModelRepo(repoUri: String): KijiModelRepository = {
    withKiji(KijiURI.newBuilder(repoUri).build, HBaseConfiguration.create()) {
      kiji => KijiModelRepository.open(kiji)
    }
  }
}
