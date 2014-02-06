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


/** GET the URL for a model by name, relative to the base URL of the server. */
@ApiAudience.Public
@ApiStability.Experimental
class GetModelServlet extends HttpServlet {
  import GetModelServlet._

  override def doGet(
      request: HttpServletRequest,
      response: HttpServletResponse
  ) {
    val model: ArtifactName = new ArtifactName(request.getParameter("model"))
    val models = getServletContext
        .getAttribute(ModelRepoScannerServlet.ModelsListContextKey)
        .asInstanceOf[mutable.Map[ArtifactName, File]]
    val modelUrl: Option[String] = if (models.contains(model)) {
      Some(ModelRepoScannerServlet.contextPathFromArtifact(model))
    } else {
      None
    }

    writeResponse(model.getFullyQualifiedName, modelUrl, response)
  }
}

@ApiAudience.Public
@ApiStability.Experimental
object GetModelServlet {
  val LOG: Logger = LoggerFactory.getLogger(classOf[GetModelServlet])

  /**
   * Write the retrieved model name into the http response.
   *
   * @param model whose relative endpoint was retrieved.
   * @param modelUrl Some(model_endpoint) or None if the model does not exist.
   * @param response into which the model endpoint will be written.
   */
  private def writeResponse(
    model: String,
    modelUrl: Option[String],
    response: HttpServletResponse
  ) {
    doAndClose(new OutputStreamWriter(response.getOutputStream, "UTF-8")) {
      osw: OutputStreamWriter => doAndClose(new BufferedWriter(osw)) {
        bw: BufferedWriter => modelUrl match {
          case Some(url) => bw.write("""{"%s":"%s"}""".format(model, url))
          case None => response.setStatus(400); bw.write("model: %s not found.".format(model))
        }
      }
    }
  }
}
