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

package com.cloudera.spark.client.api.java

import java.net.URL
import java.util.{Map => JMap}
import java.util.concurrent.Future

import scala.collection.JavaConverters._

import com.cloudera.spark.client._

/**
 * Defines the Java API for the Spark remote client interface.
 */
class JavaSparkClient private (private val client: SparkClient) {

  /**
   * Submits a job for asynchronous execution.
   *
   * @param job The job to execute.
   * @return A handle that be used to monitor the job.
   */
  def submit[T >: Serializable](job: JavaJob[T]): JavaJobHandle[T] = {
    new JavaJobHandle(client.submit(jc => job.call(new JavaJobContext(jc))))
  }

  /**
   * Stops the remote context.
   *
   * Any pending jobs will be cancelled, and the remote context will be torn down.
   */
  def stop(): Unit = {
    client.stop()
  }

  /**
   * Adds a jar file to the running remote context.
   *
   * @param url The location of the jar file.
   * @return A future that can be used to check when the add is complete.
   */
  def addJar(url: URL): Future[Unit] = {
    throw new Exception("NOT IMPLEMENTED")
  }

}

object JavaSparkClient {

  /** Initializes the SparkClient library. Must be called before creating client instances. */
  def initialize(conf: JMap[String, String]): Unit = SparkClient.initialize(conf.asScala.toMap)

  /** Stops the SparkClient library. */
  def uninitialize(): Unit = SparkClient.stop()

  /**
   * Instantiates a new Spark client.
   *
   * @param conf Configuration for the remote Spark application.
   */
  def createClient(conf: JMap[String, String]): JavaSparkClient =
      new JavaSparkClient(SparkClient.createClient(conf.asScala.toMap))

}
