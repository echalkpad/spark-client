/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.spark.client

import java.io.{File, FileInputStream, FileOutputStream}
import java.net.URL
import java.util.zip.ZipEntry
import java.util.jar.JarOutputStream

import scala.concurrent.Await
import scala.concurrent.duration._

import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import org.apache.spark.SparkFiles
import org.apache.spark.SparkContext._

import com.cloudera.spark.client.impl.ClientUtils

class SparkClientSuite extends FunSuite with Matchers with BeforeAndAfterAll {

  // Timeouts are bad... mmmkay.
  private val timeout = Duration(10, SECONDS)

  private val driverLogFile = "target/unit-tests-driver.log"

  override def beforeAll() = {
    // Clean up the driver log file if it exists.
    new File(driverLogFile).delete()

    super.beforeAll()
  }

  private def runTest(conf: Map[String, String], fn: SparkClient => Unit) = {
    SparkClient.initialize(conf)
    var client: SparkClient = null
    try {
      client = SparkClient.createClient(conf)
      fn(client)
    } finally {
      if (client != null) {
        client.stop()
      }
      SparkClient.stop()
    }
  }

  private def localTest(name: String, extraConf: Map[String, String] = Map())
      (fn: SparkClient => Unit) = test(name) {
    val conf = Map(
      (ClientUtils.CONF_KEY_IN_PROCESS -> "true"),
      ("spark.master" -> "local"),
      ("spark.app.name" -> "SparkClientSuite Local App"),
      ("spark.akka.logLifecycleEvents" -> "true")) ++ extraConf
    runTest(conf, fn)
  }

  private def remoteTest(name: String)(fn: SparkClient => Unit) =
    test(name) {
      // We need to propagate the classpath here since at this point there might not be an
      // actual jar file for the client.
      val classpath = sys.props("java.class.path")

      val conf = Map(
        ("spark.driver.extraJavaOptions" ->
          s"-Dspark.test.log.file=$driverLogFile -Dspark.test.log.file.append=true"),
        ("spark.master" -> "local"),
        ("spark.app.name" -> "SparkClientSuite Remote App"),
        ("spark.driver.extraClassPath" -> classpath),
        ("spark.executor.extraClassPath" -> classpath))
      runTest(conf, fn)
    }

  localTest("basic job submission") { client =>
    val res = Await.result(client.submit { (jc) => "hello" }, timeout)
    res should be ("hello")
  }

  localTest("client tests in local mode") { client =>
    runSimpleJob(client)

    // Make sure exceptions are reported.
    try {
      Await.result(client.submit { jc => throw new ArithmeticException("Bad at math.") }, timeout)
      throw new IllegalStateException("Should not reach here.")
    } catch {
      case e: ArithmeticException =>
        // All is good.
    }

    // Make sure throwables are caught.
    try {
      Await.result(client.submit { jc => throw new LinkageError("An error! Boo!") }, timeout)
      throw new IllegalStateException("Should not reach here.")
    } catch {
      case e: LinkageError =>
        // All is good.
    }

    // Should still be able to submit jobs after a failed one.
    runSimpleJob(client)
  }

  localTest("metrics collection") { client =>
    val future = runSimpleJob(client)
    val metrics = future.metrics
    metrics.getJobIds().size should be (1)
    metrics.getAllMetrics().executorRunTime should be > 0L

    val future2 = runSimpleJob(client)
    val metrics2 = future2.metrics
    metrics2.getJobIds().size should be (1)
    metrics.getJobIds() should not be (metrics2.getJobIds())
    metrics2.getAllMetrics().executorRunTime should be > 0L
  }

  localTest("add jars and files") { client =>
    var jar: Option[File] = None
    var file: Option[File]  = None

    try {
      // Test that adding a jar to the remote context makes it show up in the classpath.
      jar = Some(File.createTempFile("test", ".jar"))

      val jarFile = new JarOutputStream(new FileOutputStream(jar.get))
      jarFile.putNextEntry(new ZipEntry("test.resource"))
      jarFile.write("test resource".getBytes("UTF-8"))
      jarFile.closeEntry()
      jarFile.close()

      Await.ready(client.addJar(new URL("file:" + jar.get.getAbsolutePath())), timeout)

      val jarJob = client.submit { jc =>
        // Need to run a Spark job to make sure the jar is added to the class loader. Monitoring
        // SparkContext#addJar() doesn't mean much, we can only be sure jars have been distributed
        // when we run a task after the jar has been added.
        jc.sc.parallelize(Seq(1)).map { _ =>
          val ccl = Thread.currentThread().getContextClassLoader()
          val in = ccl.getResourceAsStream("test.resource")
          val bytes = new Array[Byte](1024)

          var count = 0
          var read = in.read(bytes)
          while (read != -1) {
            count += read
            read = in.read(bytes, count, bytes.length - count)
          }
          in.close()
          new String(bytes, 0, count, "UTF-8")
        }.collect()(0)
      }
      var result = Await.result(jarJob, timeout)
      result should be ("test resource")

      // Test that adding a file to the remote context makes it available to executors.
      file = Some(File.createTempFile("test", ".file"))

      val fileStream = new FileOutputStream(file.get)
      fileStream.write("test file".getBytes("UTF-8"))
      fileStream.close()

      Await.ready(client.addJar(new URL("file:" + file.get.getAbsolutePath())), timeout)

      val fileName = file.get.getName()
      val fileJob = client.submit { jc =>
        // The same applies to files added with "addFile". They're only guaranteed to be available
        // to tasks started after the addFile() call completes.
        jc.sc.parallelize(Seq(1)).map { _ =>
          val in = new FileInputStream(SparkFiles.get(fileName))
          val bytes = new Array[Byte](1024)

          var count = 0
          var read = in.read(bytes)
          while (read != -1) {
            count += read
            read = in.read(bytes, count, bytes.length - count)
          }
          in.close()
          new String(bytes, 0, count, "UTF-8")
        }.collect()(0)
      }

      val fileResult = Await.result(fileJob, timeout)
      fileResult should be ("test file")
    } finally {
      jar.foreach(_.delete())
      file.foreach(_.delete())
    }
  }

  localTest("kryo serializer", Map(ClientUtils.CONF_KEY_SERIALIZER -> "kryo")) { client =>
    runSimpleJob(client)
  }

  remoteTest("basic remote job submission") { client =>
    val res = Await.result(client.submit { (jc) => "hello" }, timeout)
    res should be ("hello")
  }

  private def runSimpleJob(client: SparkClient) = {
    val future = client.submit { jc =>
      val rdd = jc.sc.parallelize(1 to 10)
      Await.result(jc.monitor(rdd.countAsync()), Duration.Inf)
    }
    val res = Await.result(future, timeout)
    res should be (10)
    future
  }

}
