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

import org.apache.spark.executor.TaskMetrics

/**
 * Metrics tracked during the execution of a job.
 *
 * Depending on how the metrics object is obtained (by calling methods in the `MetricsCollection`
 * class), metrics will refer to one or more tasks.
 *
 * @param executorDeserializeTime Time taken on the executor to deserialize tasks.
 * @param executorRunTime Time the executor spends actually running the task (including fetching
 *                        shuffle data).
 * @param resultSize The number of bytes sent back to the driver by tasks.
 * @param jvmGCTime Amount of time the JVM spent in garbage collection while executing tasks.
 * @param resultSerializationTime Amount of time spent serializing task results.
 * @param memoryBytesSpilled The number of in-memory bytes spilled by tasks.
 * @param diskBytesSpilled The number of on-disk bytes spilled by tasks.
 * @param inputMetrics If tasks read from a HadoopRDD or from persisted data, metrics on how
 *                     much data was read.
 * @param shuffleReadMetrics If tasks read from shuffle output, metrics on getting shuffle data.
 *                           This includes read metrics aggregated over all the tasks' shuffle
 *                           dependencies.
 * @param shuffleWriteMetrics If tasks wrote to shuffle output, metrics on the written shuffle data.
 */
class Metrics private[client] (
    val executorDeserializeTime: Long,
    val executorRunTime: Long,
    val resultSize: Long,
    val jvmGCTime: Long,
    val resultSerializationTime: Long,
    val memoryBytesSpilled: Long,
    val diskBytesSpilled: Long,
    val inputMetrics: Option[InputMetrics],
    val shuffleReadMetrics: Option[ShuffleReadMetrics],
    val shuffleWriteMetrics: Option[ShuffleWriteMetrics]) extends Serializable {

  private[client] def this(metrics: TaskMetrics) = {
    this(
      metrics.executorDeserializeTime,
      metrics.executorRunTime,
      metrics.resultSize,
      metrics.jvmGCTime,
      metrics.resultSerializationTime,
      metrics.memoryBytesSpilled,
      metrics.diskBytesSpilled,
      metrics.inputMetrics.map(im => new InputMetrics(metrics)),
      metrics.shuffleReadMetrics.map(srm => new ShuffleReadMetrics(metrics)),
      metrics.shuffleWriteMetrics.map(swm => new ShuffleWriteMetrics(metrics)))
  }

}

/**
 * Method by which input data was read.  Network means that the data was read over the network
 * from a remote block manager (which may have stored the data on-disk or in-memory).
 */
object DataReadMethod extends Enumeration {
  type DataReadMethod = Value
  val Memory, Disk, Hadoop, Network, Multiple = Value

}

/**
 * Metrics pertaining to reading input data.
 */
class InputMetrics private[client] (
    val readMethod: DataReadMethod.Value,
    val bytesRead: Long) extends Serializable {

  private[client] def this(metrics: TaskMetrics) = {
    this(DataReadMethod.withName(metrics.inputMetrics.get.readMethod.toString),
      metrics.inputMetrics.get.bytesRead)
  }

}

/**
 * Metrics pertaining to reading shuffle data.
 *
 * @param shuffleFinishTime Absolute time when tasks finished reading shuffle data.
 * @param Number of remote blocks fetched in shuffles by tasks.
 * @param Number of local blocks fetched in shuffles by tasks.
 * @param fetchWaitTime Time tasks spent waiting for remote shuffle blocks. This only includes the
 *                      time blocking on shuffle input data. For instance if block B is being
 *                      fetched while the task is still not finished processing block A, it is not
 *                      considered to be blocking on block B.
 * @param remoteBytesRead Total number of remote bytes read from the shuffle by tasks.
 */
class ShuffleReadMetrics private[client] (
    val shuffleFinishTime: Long,
    val remoteBlocksFetched: Int,
    val localBlocksFetched: Int,
    val fetchWaitTime: Long,
    val remoteBytesRead: Long) extends Serializable {

  private[client] def this(metrics: TaskMetrics) = {
    this(metrics.shuffleReadMetrics.get.shuffleFinishTime,
      metrics.shuffleReadMetrics.get.remoteBlocksFetched,
      metrics.shuffleReadMetrics.get.localBlocksFetched,
      metrics.shuffleReadMetrics.get.fetchWaitTime,
      metrics.shuffleReadMetrics.get.remoteBytesRead)
  }

  /**
   * Number of blocks fetched in shuffle by tasks (remote or local).
   */
  def totalBlocksFetched: Int = remoteBlocksFetched + localBlocksFetched

}

/**
 * Metrics pertaining to writing shuffle data.
 *
 * @param shuffleBytesWritten Number of bytes written for the shuffle by tasks.
 * @param shuffleWriteTime Time tasks spent blocking on writes to disk or buffer cache, in
 *                         nanoseconds.
 */
class ShuffleWriteMetrics private[client] (
    val shuffleBytesWritten: Long,
    val shuffleWriteTime: Long) extends Serializable {

  private[client] def this(metrics: TaskMetrics) = {
    this(metrics.shuffleWriteMetrics.get.shuffleBytesWritten,
      metrics.shuffleWriteMetrics.get.shuffleWriteTime)
  }

}
