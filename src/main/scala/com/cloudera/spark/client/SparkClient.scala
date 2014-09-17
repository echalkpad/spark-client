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

import java.net.URL

import scala.concurrent.Future

import com.cloudera.spark.client.impl._

/**
 * Defines the client interface for a remote Spark context.
 */
trait SparkClient {

  /**
   * Submits a job for asynchronous execution.
   *
   * @param job The job to execute.
   * @return A handle that be used to monitor the job.
   */
  def submit[T >: Serializable](job: JobContext => T): JobHandle[T]

  /**
   * Stops the remote context.
   *
   * Any pending jobs will be cancelled, and the remote context will be torn down.
   */
  def stop(): Unit

  /**
   * Adds a jar file to the running remote context.
   *
   * Note that the URL should be reachable by the Spark driver process. If running the driver
   * in cluster mode, it may reside on a different host, meaning "file:" URLs have to exist
   * on that node (and not on the client machine).
   *
   * @param url The location of the jar file.
   * @return A future that can be used to monitor the operation.
   */
  def addJar(url: URL): Future[_]

  /**
   * Adds a file to the running remote context.
   *
   * Note that the URL should be reachable by the Spark driver process. If running the driver
   * in cluster mode, it may reside on a different host, meaning "file:" URLs have to exist
   * on that node (and not on the client machine).
   *
   * @param url The location of the file.
   * @return A future that can be used to monitor the operation.
   */
  def addFile(url: URL): Future[_]

}

object SparkClient {

  /**
   * Initializes the SparkClient library. Must be called before creating client instances.
   *
   * @param conf Map containing configuration parameters for the client.
   */
  def initialize(conf: Map[String, String]): Unit = SparkClientImpl.initialize(conf)

  /** Stops the SparkClient library. */
  def stop(): Unit = SparkClientImpl.stop()

  /**
   * Instantiates a new Spark client.
   *
   * @param conf Configuration for the remote Spark application.
   */
  def createClient(conf: Map[String, String]): SparkClient = new SparkClientImpl(conf)

}
