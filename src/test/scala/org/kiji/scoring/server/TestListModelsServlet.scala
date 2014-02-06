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
import java.io.FileOutputStream
import java.net.URL
import java.util.jar.JarOutputStream

import org.apache.commons.io.{FileUtils, IOUtils}
import org.junit.{After, Before, Assert, Test}

import org.kiji.schema.KijiClientTest
import org.kiji.modelrepo.KijiModelRepository
import com.google.common.io.Files
import org.kiji.schema.layout.KijiTableLayouts

class TestListModelsServlet extends KijiClientTest {
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
  def testListModelsServlet() {
    val tempDir = TestUtils.setupServerEnvironment(getKiji.getURI)
    val jarFile = File.createTempFile("temp_artifact", ".jar")
    val jarOS = new JarOutputStream(new FileOutputStream(jarFile))
    TestUtils.addToJar("org/kiji/scoring/server/DummyScoreFunction.class", jarOS)
    jarOS.close()

    TestUtils.deploySampleLifecycle(getKiji, jarFile.getAbsolutePath, "0.0.1")

    val server = ScoringServer(tempDir, ServerConfiguration(8080, getKiji.getURI.toString, 0, 2))
    server.start()
    try {
      val connector = server.server.getConnectors()(0)
      TestUtils.scan(server)

      val url = new URL("http://localhost:%s/admin/list".format(connector.getLocalPort))
      val response = IOUtils.toString(url.openStream(), "UTF-8")
      Assert.assertEquals(
        """{
          |  {"org.kiji.test.sample_model-0.0.1":"models/org/kiji/test/sample_model/0.0.1"}
          |}""".stripMargin,
        response
      )
    } finally {
      server.stop()
    }
  }
}
