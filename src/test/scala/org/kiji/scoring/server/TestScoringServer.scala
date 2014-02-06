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
import java.io.FileNotFoundException
import java.io.FileOutputStream
import java.util.jar.JarOutputStream

import com.google.common.io.Files
import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang.SerializationUtils
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test

import org.kiji.modelrepo.KijiModelRepository
import org.kiji.schema.KijiClientTest
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.layout.KijiTableLayouts
import org.kiji.schema.util.InstanceBuilder

class TestScoringServer extends KijiClientTest {

  var mTempHome: File = null
  val emailAddress = "name@company.com"
  val tableLayout = KijiTableLayouts.getLayout("org/kiji/scoring/server/sample/user_table.json")

  @Before
  def setup() {
    new InstanceBuilder(getKiji)
        .withTable(tableLayout)
            .withRow(12345: java.lang.Long)
                .withFamily("info")
                    .withQualifier("email")
                        .withValue(1, emailAddress)
    .build

    val tempModelRepoDir = Files.createTempDir()
    tempModelRepoDir.deleteOnExit()
    KijiModelRepository.install(getKiji, tempModelRepoDir.toURI)

    mTempHome = TestUtils.setupServerEnvironment(getKiji.getURI)
  }

  @After
  def tearDown() {
    mTempHome.delete()
  }

  @Test
  def testShouldDeployAndRunSingleLifecycle() {
    val jarFile = File.createTempFile("temp_artifact", ".jar")
    val jarOS = new JarOutputStream(new FileOutputStream(jarFile))
    TestUtils.addToJar("org/kiji/scoring/server/DummyScoreFunction.class", jarOS)
    jarOS.close()

    TestUtils.deploySampleLifecycle(getKiji, jarFile.getAbsolutePath, "0.0.1")

    val server = ScoringServer(mTempHome, ServerConfiguration(8080, getKiji.getURI.toString, 0, 2))
    server.start()
    try {
      TestUtils.scan(server)
      val connector = server.server.getConnectors()(0)
      val response = TestUtils.scoringServerResponse(connector.getLocalPort,
        "org/kiji/test/sample_model/0.0.1/?eid=[12345]&request=" +
            Base64.encodeBase64String(SerializationUtils.serialize(KijiDataRequest.empty())))
      assert(Integer.parseInt(response.getValue) == emailAddress.length())
    } finally {
      server.stop()
    }
  }

  @Test
  def testShouldHotUndeployModelLifecycle() {
    val jarFile = File.createTempFile("temp_artifact", ".jar")
    val jarOS = new JarOutputStream(new FileOutputStream(jarFile))
    TestUtils.addToJar("org/kiji/scoring/server/DummyScoreFunction.class", jarOS)
    jarOS.close()

    TestUtils.deploySampleLifecycle(getKiji, jarFile.getAbsolutePath, "0.0.1")

    val server = ScoringServer(mTempHome, ServerConfiguration(8080, getKiji.getURI.toString, 0, 2))
    server.start()
    try {
      val connector = server.server.getConnectors()(0)
      TestUtils.scan(server)
      val response = TestUtils.scoringServerResponse(connector.getLocalPort,
        "org/kiji/test/sample_model/0.0.1/?eid=[12345]&request=" +
            Base64.encodeBase64String(SerializationUtils.serialize(KijiDataRequest.empty())))

      assert(Integer.parseInt(response.getValue.toString) == emailAddress.length())

      val modelRepoTable = getKiji.openTable("model_repo")
      try {
        val writer = modelRepoTable.openTableWriter()
        try {
          writer.put(modelRepoTable.getEntityId(TestUtils.artifactName,
            "0.0.1"), "model", "production_ready", false)
        } finally {
          writer.close()
        }
        modelRepoTable.release()
        TestUtils.scan(server)
        try {
          TestUtils.scoringServerResponse(connector.getLocalPort,
            "org/kiji/test/sample_model/0.0.1/?eid=[12345]&request=" +
                Base64.encodeBase64String(SerializationUtils.serialize(KijiDataRequest.empty())))
          Assert.fail("Scoring server should have thrown a 404 but didn't")
        } catch {
          case ex: FileNotFoundException => ()
        }
      }
    } finally {
      server.stop()
    }
  }

  @Test
  def testCanPassFreshParameters() {
    val jarFile = File.createTempFile("temp_artifact", ".jar")
    val jarOS = new JarOutputStream(new FileOutputStream(jarFile))
    TestUtils.addToJar("org/kiji/scoring/server/DummyScoreFunction.class", jarOS)
    jarOS.close()

    TestUtils.deploySampleLifecycle(getKiji, jarFile.getAbsolutePath, "0.0.1")

    val server = ScoringServer(mTempHome, ServerConfiguration(8080, getKiji.getURI.toString, 0, 2))
    server.start()
    try {
      val connector = server.server.getConnectors()(0)
      TestUtils.scan(server)
      // "%3D%3D%3D is a url encoding of '==='.
      val response = TestUtils.scoringServerResponse(connector.getLocalPort,
        "org/kiji/test/sample_model/0.0.1/?eid=[12345]&fresh.jennyanydots=%3D%3D%3D&request=" +
            Base64.encodeBase64String(SerializationUtils.serialize(KijiDataRequest.empty())))
      assert(Integer.parseInt(response.getValue.toString) == "===".length())
    } finally {
      server.stop()
    }
  }


}
