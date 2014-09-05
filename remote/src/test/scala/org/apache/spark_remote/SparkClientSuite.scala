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

package org.apache.spark_remote

import scala.concurrent.Await
import scala.concurrent.duration._

import org.scalatest.{FunSuite, Matchers}

import org.apache.spark.SparkConf
import org.apache.spark_remote.impl.ClientUtils

class SparkClientSuite extends FunSuite with Matchers {

  // Timeouts are bad... mmmkay.
  private val timeout = Duration(10, SECONDS)

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

  private def localTest(name: String)(fn: SparkClient => Unit) =
    test(name) {
      val conf = Map(
        (ClientUtils.CONF_KEY_IN_PROCESS -> "true"),
        ("spark.master" -> "local"),
        ("spark.app.name" -> "SparkClientSuite Local App"))
      runTest(conf, fn)
    }

  private def remoteTest(name: String)(fn: SparkClient => Unit) =
    test(name) {
      val sparkHome = sys.props.getOrElse("spark.test.home", fail("spark.test.home is not set!"))

      // We need to propagate the classpath here since at this point there might not be an
      // actual jar file for the client.
      val classpath = sys.props("java.class.path")

      val conf = Map(
        ("spark.master" -> "local"),
        ("spark.app.name" -> "SparkClientSuite Remote App"),
        ("spark.home" -> sparkHome),
        ("spark.driver.extraClassPath" -> classpath),
        ("spark.executor.extraClassPath" -> classpath))
      runTest(conf, fn)
    }

  localTest("basic job submission") { case client =>
    val res = Await.result(client.submit { (jc) => "hello" }, timeout)
    res should be ("hello")
  }

  localTest("basic Spark job") { case client =>
    val future = client.submit { jc =>
      val rdd = jc.sc.parallelize(1 to 10)
      rdd.count()
    }
    val res = Await.result(future, timeout)
    res should be (10)
  }

  remoteTest("basic remote job submission") { case client =>
    val res = Await.result(client.submit { (jc) => "hello" }, timeout)
    res should be ("hello")
  }

}
