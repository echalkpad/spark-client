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

import scala.concurrent.Await
import scala.concurrent.duration._

import org.scalatest.{FunSuite, Matchers}
import org.apache.spark.SparkConf

import com.cloudera.spark.client.impl.ClientUtils

class MetricsCollectionSuite extends FunSuite with Matchers {

  test("metrics aggregation") {
    val collection = new MetricsCollection()
    // 2 jobs, 2 stages per job, 2 tasks per stage.
    (1 to 2).foreach { i =>
      (1 to 2).foreach { j =>
        (1 to 2).foreach { k =>
          collection.addMetrics(i, j, k, makeMetrics(i, j, k))
        }
      }
    }

    collection.getJobIds() should be (Set(1, 2))
    collection.getStageIds(1) should be (Set(1, 2))
    collection.getTaskIds(1, 1) should be (Set(1, 2))

    val task112 = collection.getTaskMetrics(1, 1, 2)
    checkMetrics(task112, taskValue(1, 1, 2), taskValue(1, 1, 2))

    val stage21 = collection.getStageMetrics(2, 1)
    checkMetrics(stage21, stageValue(2, 1, 2), taskValue(2, 1, 2))

    val job1 = collection.getJobMetrics(1)
    checkMetrics(job1, jobValue(1, 2, 2), taskValue(1, 2, 2))

    val global = collection.getAllMetrics()
    checkMetrics(global, globalValue(2, 2, 2), taskValue(2, 2, 2))
  }

  test("optional metrics") {
    val value = taskValue(1, 1, 1)
    val metrics = new Metrics(value, value, value, value, value, value, value, None, None, None)

    val collection = new MetricsCollection()
    (1 to 2).foreach { i => collection.addMetrics(i, 1, 1, metrics) }

    val global = collection.getAllMetrics()
    global.inputMetrics should be (None)
    global.shuffleReadMetrics should be (None)
    global.shuffleWriteMetrics should be (None)

    collection.addMetrics(3, 1, 1, makeMetrics(3, 1, 1))

    val global2 = collection.getAllMetrics()
    global2.inputMetrics should not be (None)
    global2.inputMetrics.get.bytesRead should be (taskValue(3, 1, 1))

    global2.shuffleReadMetrics should not be (None)
    global2.shuffleWriteMetrics should not be (None)
  }

  test("input read method aggregation") {
    val collection = new MetricsCollection()

    val value = taskValue(1, 1, 1)
    val metrics1 = new Metrics(value, value, value, value, value, value, value,
      Some(new InputMetrics(DataReadMethod.Memory, value)), None, None)
    val metrics2 = new Metrics(value, value, value, value, value, value, value,
      Some(new InputMetrics(DataReadMethod.Disk, value)), None, None)

    collection.addMetrics(1, 1, 1, metrics1)
    collection.addMetrics(1, 1, 2, metrics2)

    val global = collection.getAllMetrics()
    global.inputMetrics should not be (None)
    global.inputMetrics.get.readMethod should be (DataReadMethod.Multiple)
  }

  private def makeMetrics(jobId: Int, stageId: Int, taskId: Int) = {
    val value = 1000000 * jobId + 1000 * stageId + taskId
    new Metrics(value, value, value, value, value, value, value,
      Some(new InputMetrics(DataReadMethod.Memory, value)),
      Some(new ShuffleReadMetrics(value, value, value, value, value)),
      Some(new ShuffleWriteMetrics(value, value)))
  }

  /**
   * The metric values will all be the same. This makes it easy to calculate the aggregated values
   * of jobs and stages without fancy math.
   */
  private def taskValue(jobId: Int, stageId: Int, taskId: Int) = {
    1000000 * jobId + 1000 * stageId + taskId
  }

  private def stageValue(jobId: Int, stageId: Int, taskCount: Int) = {
    (1 to taskCount).map(i => taskValue(jobId, stageId, i)).reduce(_ + _)
  }

  private def jobValue(jobId: Int, stageCount: Int, tasksPerStage: Int) = {
    (1 to stageCount).map(i => stageValue(jobId, i, tasksPerStage)).reduce(_ + _)
  }

  private def globalValue(jobCount: Int, stagesPerJob: Int, tasksPerStage: Int) = {
    (1 to jobCount).map(i => jobValue(i, stagesPerJob, tasksPerStage)).reduce(_ + _)
  }

  private def checkMetrics(metrics: Metrics, expected: Long, max: Long) = {
    metrics.executorDeserializeTime should be (expected)
    metrics.executorRunTime should be (expected)
    metrics.resultSize should be (expected)
    metrics.jvmGCTime should be (expected)
    metrics.resultSerializationTime should be (expected)
    metrics.memoryBytesSpilled should be (expected)
    metrics.diskBytesSpilled should be (expected)

    val im = metrics.inputMetrics.getOrElse(throw new IllegalStateException("no input metrics"))
    im.readMethod should be (DataReadMethod.Memory)
    im.bytesRead should be (expected)

    val srm = metrics.shuffleReadMetrics.getOrElse(
      throw new IllegalStateException("no shuffle read metrics"))
    srm.shuffleFinishTime should be (max)
    srm.remoteBlocksFetched should be (expected)
    srm.localBlocksFetched should be (expected)
    srm.fetchWaitTime should be (expected)
    srm.remoteBytesRead should be (expected)

    val swm = metrics.shuffleWriteMetrics.getOrElse(
      throw new IllegalStateException("no shuffle write metrics"))
    swm.shuffleBytesWritten should be (expected)
    swm.shuffleWriteTime should be (expected)
  }

}
