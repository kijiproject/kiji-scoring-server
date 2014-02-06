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

import java.io.BufferedWriter
import java.io.File
import java.io.OutputStreamWriter
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import scala.collection.mutable

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.flow.util.ResourceUtil.doAndClose
import org.kiji.modelrepo.ArtifactName
import org.kiji.scoring.server.Templates

/**
 * GET a list of the models deployed by this ScoringServer mapped to their URLs relative to the base
 * URL of the server.
 */
@ApiAudience.Public
@ApiStability.Experimental
class ListModelsServlet extends HttpServlet {
  import ListModelsServlet._

  override def doGet(
      request: HttpServletRequest,
      response: HttpServletResponse
  ) {
    val models = getServletContext
        .getAttribute(ModelRepoScannerServlet.ModelsListContextKey)
        .asInstanceOf[mutable.Map[ArtifactName, File]]
        .keySet
        .map { artifactName => (artifactName.getFullyQualifiedName,
            ModelRepoScannerServlet.contextPathFromArtifact(artifactName))
        }.toMap

    writeResponse(models, response)
  }
}

@ApiAudience.Public
@ApiStability.Experimental
object ListModelsServlet {
  val LOG: Logger = LoggerFactory.getLogger(classOf[ListModelsServlet])

  /**
   * Write the deployed models into the http response.
   *
   * @param models currently deployed by this server mapped to their relative endpoints.
   * @param response into which the models will be written.
   */
  private def writeResponse(
    models: Map[String, String],
    response: HttpServletResponse
  ) {
    doAndClose(new OutputStreamWriter(response.getOutputStream, "UTF-8")) {
      osw: OutputStreamWriter => doAndClose(new BufferedWriter(osw)) {
        bw: BufferedWriter => bw.write(Templates.mapToJson(models))
      }
    }
  }
}
