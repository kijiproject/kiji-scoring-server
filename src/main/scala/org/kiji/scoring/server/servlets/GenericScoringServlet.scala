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
import java.io.OutputStreamWriter
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import java.util.{Map => JMap}
import scala.collection.JavaConverters.enumerationAsScalaIteratorConverter
import scala.collection.JavaConverters.mapAsJavaMapConverter

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.base.Preconditions
import com.google.gson.Gson
import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang.SerializationUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.util.ReflectionUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.flow.util.ResourceUtil.doAndClose
import org.kiji.express.flow.util.ResourceUtil.withKijiTable
import org.kiji.mapreduce.kvstore.KeyValueStoreReaderFactory
import org.kiji.schema.{EntityId => JEntityId}
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiTableReader
import org.kiji.schema.KijiTableReaderPool
import org.kiji.schema.KijiURI
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.tools.ToolUtils
import org.kiji.scoring.ScoreFunction
import org.kiji.scoring.ScoreFunction.TimestampedValue
import org.kiji.scoring.impl.InternalFreshenerContext // TODO make this framework instead of private
import org.kiji.scoring.server.KijiScoringServerCell

/**
 * GET a score for particular model.
 *
 * @tparam T type of the value returned by the model.
 */
@ApiAudience.Public
@ApiStability.Experimental
class GenericScoringServlet[T] extends HttpServlet {
  import GenericScoringServlet._

  // Set during init().
  private var attachedTableLayout: KijiTableLayout = null
  private var readerPool: KijiTableReaderPool = null
  private var scoreFunction: ScoreFunction[T] = null
  private var attachedColumn: KijiColumnName = null
  private var recordParameters: JMap[String, String] = null
  private var kvFactory: KeyValueStoreReaderFactory = null

  override def init() {
    val tableURI: KijiURI = KijiURI.newBuilder(getInitParameter(TABLE_URI_KEY)).build()

    val (layout, pool) = withKijiTable(tableURI, HBaseConfiguration.create()) {
      table: KijiTable => {
        val pool = KijiTableReaderPool.Builder.create()
            .withExhaustedAction(KijiTableReaderPool.Builder.WhenExhaustedAction.GROW)
            .withReaderFactory(table.getReaderFactory)
            .build()
        (table.getLayout, pool)
      }
    }
    attachedTableLayout = layout
    readerPool = pool

    val sfClassName = getServletConfig.getInitParameter(SCORE_FUNCTION_CLASS_KEY)
    val sfClass = Class.forName(sfClassName).asSubclass(classOf[ScoreFunction[T]])
    scoreFunction = ReflectionUtils.newInstance(sfClass, null)
    attachedColumn = new KijiColumnName(getServletConfig.getInitParameter(ATTACHED_COLUMN_KEY))
    recordParameters = GSON.fromJson(
        getServletConfig.getInitParameter(RECORD_PARAMETERS_KEY),
        classOf[JMap[String, String]]
    )
    val setupContext: InternalFreshenerContext =
        InternalFreshenerContext.create(attachedColumn, recordParameters)
    kvFactory = KeyValueStoreReaderFactory.create(scoreFunction.getRequiredStores(setupContext))
    setupContext.setKeyValueStoreReaderFactory(kvFactory)
    scoreFunction.setup(setupContext)
  }

  override def destroy() {
    val cleanupContext: InternalFreshenerContext =
        InternalFreshenerContext.create(attachedColumn, recordParameters, kvFactory)
    scoreFunction.cleanup(cleanupContext)
    kvFactory.close()
    readerPool.close()
  }

  override def doGet(
      request: HttpServletRequest,
      response: HttpServletResponse
  ) {
    LOG.debug("Request received: {}", request)
    // Fetch the entity_id parameter from the URL. Fail if not specified. The empty string
    // disambiguates the method reference.
    val eidString: String =
        Preconditions.checkNotNull(request.getParameter("eid"), "Entity ID required!%s", "")
    val entityId: JEntityId =
      ToolUtils.createEntityIdFromUserInputs(eidString, attachedTableLayout)

    // Fetch a map of request parameters.
    val parameterOverrides: JMap[String, String] = request
        .getParameterNames
        .asScala
        .collect {
          case param: String if param.startsWith("fresh.") =>
              (param.stripPrefix("fresh."), request.getParameter(param))
        }
        .toMap
        .asJava

    // Deserialize the client data request.
    val clientDataRequest: KijiDataRequest =
        deserializeKijiDataRequest(request.getParameter("request"))

    val output: KijiScoringServerCell = score(clientDataRequest, parameterOverrides, entityId)
    writeResponse(output, response)
  }


  /**
   * Score the given entity with the model contained in this servlet.
   *
   * @param clientDataRequest which triggered this freshening.
   * @param parameterOverrides from the request.
   * @param entityId of the row to score.
   * @return a KijiScoringServerCell which will be returned to the http caller.
   */
  private def score(
      clientDataRequest: KijiDataRequest,
      parameterOverrides: JMap[String, String],
      entityId: JEntityId
  ): KijiScoringServerCell = {
    val context: InternalFreshenerContext = InternalFreshenerContext.create(
      clientDataRequest,
      attachedColumn,
      recordParameters,
      parameterOverrides,
      kvFactory
    )

    val rowData: KijiRowData = {
      val reader: KijiTableReader = readerPool.borrowObject()
      try {
        reader.get(entityId, scoreFunction.getDataRequest(context))
      } finally {
        reader.close()
      }
    }

    val score: TimestampedValue[T] = scoreFunction.score(rowData, context)

    new KijiScoringServerCell(
      attachedColumn.getFamily,
      attachedColumn.getQualifier,
      score.getTimestamp,
      score.getValue
    )
  }
}

/** General resources used by all instances of GenericScoringServlet. */
@ApiAudience.Public
@ApiStability.Experimental
object GenericScoringServlet {
  val LOG: Logger = LoggerFactory.getLogger(classOf[GenericScoringServlet[_]])
  val MAPPER: ObjectMapper = new ObjectMapper()
  val GSON: Gson = new Gson

  val RECORD_PARAMETERS_KEY = "record_parameters"
  val ATTACHED_COLUMN_KEY = "attached_column"
  val SCORE_FUNCTION_CLASS_KEY = "score_function_class"
  val TABLE_URI_KEY = "table_uri"

  /**
   * Deserialize a base64 encoded KijiDataRequest.
   *
   * @param base64encodedRequest to deserialize.
   * @return the KijiDataRequest represented by the serialized input.
   */
  private def deserializeKijiDataRequest(
      base64encodedRequest: String
  ): KijiDataRequest = {
    val bytes: Array[Byte] = Base64.decodeBase64(base64encodedRequest)
    SerializationUtils.deserialize(bytes).asInstanceOf[KijiDataRequest]
  }

  /**
   * Write the calculated score into the http response.
   *
   * @param score to be returned to the caller.
   * @param response into which the score will be written.
   */
  private def writeResponse(
      score: KijiScoringServerCell,
      response: HttpServletResponse
  ) {
    response.setStatus(200)
    response.getWriter.print(MAPPER.valueToTree(score).toString)
  }
}
