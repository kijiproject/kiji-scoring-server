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

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/** GET and PUT the status of the server. */
@ApiAudience.Public
@ApiStability.Experimental
class PingServlet extends HttpServlet {
  import PingServlet._

  private[this] var status: Status = Healthy

  override def doGet(
    request: HttpServletRequest,
    response: HttpServletResponse
  ) {
    status match {
      case Healthy => response.setStatus(200)
      case Hidden => response.setStatus(404)
      case Unhealthy => response.setStatus(500)
    }
  }

  override def doPut(
    request: HttpServletRequest,
    response: HttpServletResponse
  ) {
    val newHealth = request.getParameter("status")
    newHealth match {
      case "Healthy" => status = Healthy; response.setStatus(200)
      case "Hidden" => status = Hidden; response.setStatus(200)
      case "Unhealthy" => status = Unhealthy; response.setStatus(200)
      case unknown => {
        response.sendError(400, "unknown server status: %s".format(unknown))
      }
    }
  }
}

@ApiAudience.Public
@ApiStability.Experimental
object PingServlet {
  /** Possible statuses of the server. */
  sealed trait Status
  case object Healthy extends Status
  case object Hidden extends Status
  case object Unhealthy extends Status

  val LOG: Logger = LoggerFactory.getLogger(classOf[PingServlet])
}
