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

import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.eclipse.jetty.overlays.OverlayedAppProvider
import org.junit.After
import org.junit.Before
import org.junit.Test

import org.kiji.express.flow.util.ResourceUtil.withKijiTable
import org.kiji.express.flow.util.ResourceUtil.withKijiTableWriter
import org.kiji.modelrepo.KijiModelRepository
import org.kiji.schema.KijiClientTest
import org.kiji.schema.layout.KijiTableLayouts

class TestModelRepoScannerServlet extends KijiClientTest {

  var tempServerDir: File = null
  var tempKijiModelRepositoryDir: File = null
  var modelRepo: KijiModelRepository = null

  @Before
  def setup() {
    tempServerDir = TestUtils.setupServerEnvironment(getKiji.getURI)
    tempKijiModelRepositoryDir = Files.createTempDir()
    KijiModelRepository.install(getKiji, tempKijiModelRepositoryDir.toURI)
    modelRepo = KijiModelRepository.open(getKiji)
    getKiji.createTable(KijiTableLayouts.getLayout(
        "org/kiji/scoring/server/sample/user_table.json"))
  }

  @After
  def tearDown() {
    modelRepo.close()
    FileUtils.deleteDirectory(tempServerDir)
    FileUtils.deleteDirectory(tempKijiModelRepositoryDir)
  }

  @Test
  def testShouldDeployASingleLifecycle() {
    val bogusArtifact = new File(tempServerDir, "conf/configuration.json").getAbsolutePath

    TestUtils.deploySampleLifecycle(getKiji, bogusArtifact, "0.0.1")

    val server = ScoringServer(
        tempServerDir, ServerConfiguration(8080, getKiji.getURI.toString, 0, 2))
    server.start()
    try {
      TestUtils.scan(server)

      val webappFile = new File(tempServerDir,
        "models/webapps/org.kiji.test.sample_model-0.0.1.war")
      val locationFile = new File(tempServerDir,
        "models/webapps/org.kiji.test.sample_model-0.0.1.war.loc")
      val templateDir = new File(tempServerDir,
        "models/templates/org.kiji.test.sample_model-0.0.1=org.kiji.test.sample_model-0.0.1")
      val instanceDir = new File(tempServerDir,
        "models/instances/org.kiji.test.sample_model-0.0.1=org.kiji.test.sample_model-0.0.1")

      assert(webappFile.exists())
      assert(locationFile.exists())
      assert(templateDir.exists())
      assert(instanceDir.exists())
    } finally {
      server.stop()
    }
  }

  @Test
  def testShouldUndeployASingleLifecycle() {
    val bogusArtifact = new File(tempServerDir, "conf/configuration.json").getAbsolutePath

    TestUtils.deploySampleLifecycle(getKiji, bogusArtifact, "0.0.1")

    val server = ScoringServer(
        tempServerDir, ServerConfiguration(8080, getKiji.getURI.toString, 0, 2))
    server.start()
    try {
      TestUtils.scan(server)

      val instanceDir = new File(tempServerDir,
        "models/instances/org.kiji.test.sample_model-0.0.1=org.kiji.test.sample_model-0.0.1")
      assert(instanceDir.exists())

      // For now undeploy will delete the instance directory
      val modelRepoTable = getKiji.openTable(KijiModelRepository.MODEL_REPO_TABLE_NAME)
      val writer = modelRepoTable.openTableWriter()
      val eid = modelRepoTable.getEntityId("org.kiji.test.sample_model", "0.0.1")
      writer.put(eid, "model", "production_ready", false)
      writer.close()
      modelRepoTable.release()
      TestUtils.scan(server)
      assert(!instanceDir.exists())
    } finally {
      server.stop()
    }
  }

  @Test
  def testShouldLinkMultipleLifecyclesToSameArtifact() {
    val bogusArtifact = new File(tempServerDir, "conf/configuration.json").getAbsolutePath

    TestUtils.deploySampleLifecycle(getKiji, bogusArtifact, "0.0.1")
    TestUtils.deploySampleLifecycle(getKiji, bogusArtifact, "0.0.2")

    // Force the location of 0.0.2 to be that of 0.0.1
    withKijiTable(getKiji, KijiModelRepository.MODEL_REPO_TABLE_NAME) {
      table => withKijiTableWriter(table) {
        writer => {
          val eid = table.getEntityId("org.kiji.test.sample_model", "0.0.2")
          writer.put(
              eid,
              "model",
              "location",
              "org/kiji/test/sample_model/0.0.1/sample_model-0.0.1.war"
          )
        }
      }
    }

    val server = ScoringServer(
        tempServerDir, ServerConfiguration(8080, getKiji.getURI.toString, 0, 2))
    server.start()
    try {
      TestUtils.scan(server)

      assert(new File(tempServerDir, "models/instances/"
          + "org.kiji.test.sample_model-0.0.1=org.kiji.test.sample_model-0.0.1").exists())

      assert(new File(tempServerDir, "models/instances/"
          + "org.kiji.test.sample_model-0.0.1=org.kiji.test.sample_model-0.0.2").exists())

      assert(new File(tempServerDir, "models/webapps/"
          + "org.kiji.test.sample_model-0.0.1.war").exists())
      assert(!new File(tempServerDir, "models/webapps/"
          + "org.kiji.test.sample_model-0.0.2.war").exists())
    } finally {
      server.stop()
    }
  }

  @Test
  def testShouldUndeployAnArtifactAfterMultipleLifecyclesAreDeployed() {
    val bogusArtifact = new File(tempServerDir, "conf/configuration.json").getAbsolutePath

    TestUtils.deploySampleLifecycle(getKiji, bogusArtifact, "0.0.1")
    TestUtils.deploySampleLifecycle(getKiji, bogusArtifact, "0.0.2")

    // Force the location of 0.0.2 to be that of 0.0.1
    val modelRepoTable = getKiji.openTable(KijiModelRepository.MODEL_REPO_TABLE_NAME)
    try {
      val eid = modelRepoTable.getEntityId("org.kiji.test.sample_model", "0.0.2")
      val writer = modelRepoTable.openTableWriter()
      try {
        writer.put(eid, "model", "location",
            "org/kiji/test/sample_model/0.0.1/sample_model-0.0.1.war")

        val server = ScoringServer(
          tempServerDir, ServerConfiguration(8080, getKiji.getURI.toString, 0, 2))
        server.start()
        try {
          TestUtils.scan(server)

          assert(new File(tempServerDir, "models/instances/"
              + "org.kiji.test.sample_model-0.0.1=org.kiji.test.sample_model-0.0.1").exists())
          assert(new File(tempServerDir, "models/instances/"
              + "org.kiji.test.sample_model-0.0.1=org.kiji.test.sample_model-0.0.2").exists())

          assert(new File(tempServerDir, "models/webapps/"
              + "org.kiji.test.sample_model-0.0.1.war").exists())
          assert(new File(tempServerDir, "models/webapps/"
              + "org.kiji.test.sample_model-0.0.1.war.loc").exists())
          assert(!new File(tempServerDir, "models/webapps/"
              + "org.kiji.test.sample_model-0.0.2.war").exists())

          writer.put(eid, "model", "production_ready", false)
          TestUtils.scan(server)
          assert(!new File(tempServerDir, "models/instances/"
              + "org.kiji.test.sample_model-0.0.2=org.kiji.test.sample_model-0.0.2").exists())
        } finally {
          server.stop()
        }
      } finally {
        writer.close()
      }
    } finally {
      modelRepoTable.release()
    }

  }

  @Test
  def testShouldResurrectStateFromDisk() {
    testShouldDeployASingleLifecycle()

    val server = ScoringServer(
        tempServerDir, ServerConfiguration(8080, getKiji.getURI.toString, 0, 2))
    server.start()
    try {
      TestUtils.scan(server)

      // Now let's make sure that the state is right by deploying a 0.0.2 that should rely on
      // state of 0.0.1 in the scanner to make sure that we deploy things correctly.
      val bogusArtifact = new File(tempServerDir, "conf/configuration.json").getAbsolutePath
      TestUtils.deploySampleLifecycle(getKiji, bogusArtifact, "0.0.2")

      // Force the location of 0.0.2 to be that of 0.0.1
      withKijiTable(getKiji, KijiModelRepository.MODEL_REPO_TABLE_NAME) {
        table => withKijiTableWriter(table) {
          writer => {
            val eid = table.getEntityId("org.kiji.test.sample_model", "0.0.2")
            writer.put(
                eid,
                "model",
                "location",
                "org/kiji/test/sample_model/0.0.1/sample_model-0.0.1.war"
            )
          }
        }
      }
      TestUtils.scan(server)

      assert(new File(tempServerDir, "models/instances/"
          + "org.kiji.test.sample_model-0.0.1=org.kiji.test.sample_model-0.0.1").exists())

      assert(new File(tempServerDir, "models/instances/"
          + "org.kiji.test.sample_model-0.0.1=org.kiji.test.sample_model-0.0.2").exists())

      assert(new File(tempServerDir, "models/webapps/"
          + "org.kiji.test.sample_model-0.0.1.war").exists())
      assert(!new File(tempServerDir, "models/webapps/"
          + "org.kiji.test.sample_model-0.0.2.war").exists())
    } finally {
      server.stop()
    }
  }

  @Test
  def testShouldRemoveInvalidArtifacts() {
    val server = ScoringServer(
        tempServerDir, ServerConfiguration(8080, getKiji.getURI.toString, 0, 2))
    server.start()
    try {
      TestUtils.scan(server)

      // Create a few invalid webapps. Some directories, some files.
      val webappsFolder = new File(tempServerDir,
        new File(ScoringServer.MODELS_FOLDER, OverlayedAppProvider.WEBAPPS).getPath)
      assert(new File(webappsFolder, "invalid_war.war").createNewFile())
      assert(new File(webappsFolder, "invalid_war_folder").mkdir())
      assert(new File(webappsFolder, "invalid_war.txt").createNewFile())

      // Create a few invalid template folders and files
      val templatesFolder = new File(tempServerDir,
        new File(ScoringServer.MODELS_FOLDER, OverlayedAppProvider.TEMPLATES).getPath)
      assert(new File(templatesFolder, "invalid_template.txt").createNewFile())
      assert(new File(templatesFolder, "invalid_template_dir").mkdir())
      assert(new File(templatesFolder, "invalidname=invalidvalue").mkdir())

      // Create a few invalid instances
      val instancesFolder = new File(tempServerDir,
        new File(ScoringServer.MODELS_FOLDER, OverlayedAppProvider.INSTANCES).getPath)
      assert(new File(instancesFolder, "invalid_template.txt").createNewFile())
      assert(new File(instancesFolder, "invalid_template_dir").mkdir())
      assert(new File(instancesFolder, "invalidname=invalidvalue").mkdir())

      // Starting a second server will clean the directories of invalid models.
      val server2 = ScoringServer(
        tempServerDir, ServerConfiguration(8081, getKiji.getURI.toString, 0, 2))
      server2.start()
      server2.stop()

      // Make sure that webapps don't have any of the invalid stuff
      assert(webappsFolder.list().size == 0)
      assert(instancesFolder.list().size == 0)
      assert(templatesFolder.list().size == 0)
    } finally {
      server.stop()
    }
  }
}
