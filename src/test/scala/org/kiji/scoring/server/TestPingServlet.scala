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

import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPut
import org.apache.http.impl.client.DefaultHttpClient

import org.junit.{After, Before, Assert, Test}

import org.kiji.schema.KijiClientTest
import com.google.common.io.Files
import org.kiji.modelrepo.KijiModelRepository
import org.kiji.schema.layout.KijiTableLayouts
import org.apache.commons.io.FileUtils
import java.io.File

class TestPingServlet extends KijiClientTest {

  var tempHome: File = null
  var tempKijiModelRepositoryDir: File = null
  var modelRepo: KijiModelRepository = null

  @Before
  def setup() {
    tempHome = TestUtils.setupServerEnvironment(getKiji.getURI)
    tempKijiModelRepositoryDir = Files.createTempDir()
    KijiModelRepository.install(getKiji, tempKijiModelRepositoryDir.toURI)
    modelRepo = KijiModelRepository.open(getKiji)
    getKiji.createTable(KijiTableLayouts.getLayout(
      "org/kiji/scoring/server/sample/user_table.json"))
  }

  @After
  def tearDown() {
    modelRepo.close()
    FileUtils.deleteDirectory(tempHome)
    FileUtils.deleteDirectory(tempKijiModelRepositoryDir)
  }

  @Test
  def testPingServlet() {
    val server = ScoringServer(tempHome, ServerConfiguration(0, getKiji.getURI.toString, 0, 2))
    server.start()
    try {
      val connector = server.server.getConnectors()(0)
      val url = "http://localhost:%s/admin/ping".format(connector.getLocalPort)

      {
        // The server should return pings normally.
        val client = new DefaultHttpClient()
        val get = new HttpGet(url)
        val response = client.execute(get)
        try {
          Assert.assertEquals(200, response.getStatusLine.getStatusCode)
        } finally {
          get.releaseConnection()
        }
      }
      {
        // Set it to Hidden.
        val client = new DefaultHttpClient()
        val put = new HttpPut(url + "?status=Hidden")
        val response = client.execute(put)
        try {
          Assert.assertEquals(200, response.getStatusLine.getStatusCode)
        } finally {
          put.releaseConnection()
        }
      }
      {
        // Server should be hidden.
        val client = new DefaultHttpClient()
        val get = new HttpGet(url)
        val response = client.execute(get)
        try {
          Assert.assertEquals(404, response.getStatusLine.getStatusCode)
        } finally {
          get.releaseConnection()
        }
      }
      {
        // Set it to Unhealthy.
        val client = new DefaultHttpClient()
        val put = new HttpPut(url + "?status=Unhealthy")
        val response = client.execute(put)
        try {
          Assert.assertEquals(200, response.getStatusLine.getStatusCode)
        } finally {
          put.releaseConnection()
        }
      }
      {
        // Server should be unhealthy.
        val client = new DefaultHttpClient()
        val get = new HttpGet(url)
        val response = client.execute(get)
        try {
          Assert.assertEquals(500, response.getStatusLine.getStatusCode)
        } finally {
          get.releaseConnection()
        }
      }
      {
        // Set it back to Healthy.
        val client = new DefaultHttpClient()
        val put = new HttpPut(url + "?status=Healthy")
        val response = client.execute(put)
        try {
          Assert.assertEquals(200, response.getStatusLine.getStatusCode)
        } finally {
          put.releaseConnection()
        }
      }
      {
        // Server should be Healthy.
        val client = new DefaultHttpClient()
        val get = new HttpGet(url)
        val response = client.execute(get)
        try {
          Assert.assertEquals(200, response.getStatusLine.getStatusCode)
        } finally {
          get.releaseConnection()
        }
      }
      {
        // Set it to an unknown status.
        val client = new DefaultHttpClient()
        val put = new HttpPut(url + "?status=invalid")
        val response = client.execute(put)
        try {
          Assert.assertEquals(400, response.getStatusLine.getStatusCode)
          Assert.assertEquals(
            "unknown server status: invalid", response.getStatusLine.getReasonPhrase)
        } finally {
          put.releaseConnection()
        }
      }
    } finally {
      server.stop()
    }
  }
}
