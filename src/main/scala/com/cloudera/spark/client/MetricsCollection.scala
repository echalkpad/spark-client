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

import java.util.NoSuchElementException
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable.ListBuffer

/**
 * Provides metrics collected for a submitted job.
 *
 * The collected metrics can be analysed at different levels of granularity:
 * - Global (all Spark jobs triggered by client job)
 * - Spark job
 * - Stage
 * - Task
 *
 * Only successful, non-speculative tasks are considered. Metrics are updated as tasks finish,
 * so snapshots can be retrieved before the whole job completes.
 */
class MetricsCollection {

  private val taskMetrics = new ListBuffer[(Int, Int, Long, Metrics)]()
  private val lock = new ReentrantReadWriteLock()

  def getAllMetrics(): Metrics = read { () =>
    aggregate { case _ => true }
  }

  def getJobIds(): Set[Int] = read { () =>
    taskMetrics.map { case (jId, _, _, _) => jId }.toSet
  }

  def getJobMetrics(jobId: Int): Metrics = read { () =>
    aggregate { case (jId, _, _, _) => jobId == jId }
  }

  def getStageIds(jobId: Int): Set[Int] = read { () =>
    taskMetrics.flatMap { case (jId, sId, _, _) =>
      if (jobId == jId) {
        Option(sId)
      } else {
        None
      }
    }.toSet
  }

  def getStageMetrics(jobId: Int, stageId: Int): Metrics = read { () =>
    aggregate { case (jId, sId, _, _) => (jobId == jId && stageId == sId) }
  }

  def getTaskIds(jobId: Int, stageId: Int): Set[Long] = read { () =>
    taskMetrics.flatMap { case (jId, sId, tId, _) =>
      if (jobId == jId && stageId == sId) {
        Option(tId)
      } else {
        None
      }
    }.toSet
  }

  def getTaskMetrics(jobId: Int, stageId: Int, taskId: Long): Metrics = read { () =>
    taskMetrics
      .find { case (jId, sId, tId, _) =>
        (jobId == jId) && (stageId == sId) && (taskId == tId)
      }
      .map { case (_, _, _, metrics) => metrics }
      .getOrElse(throw new NoSuchElementException("Task not found."))
  }

  private[client] def addMetrics(
      jobId: Int,
      stageId: Int,
      taskId: Long,
      metrics: Metrics): Unit = write { () =>
    val item = (jobId, stageId, taskId, metrics)
    taskMetrics += item
  }

  private def aggregate(filter: ((Int, Int, Long, Metrics)) => Boolean): Metrics = read { () =>
    // Task metrics.
    var executorDeserializeTime: Long = 0L
    var executorRunTime: Long = 0L
    var resultSize: Long = 0L
    var jvmGCTime: Long = 0L
    var resultSerializationTime: Long = 0L
    var memoryBytesSpilled: Long = 0L
    var diskBytesSpilled: Long = 0L

    // Input metrics.
    var hasInputMetrics = false
    var readMethod: DataReadMethod.Value = null
    var bytesRead: Long = 0L

    // Shuffle read metrics.
    var hasShuffleReadMetrics = false
    var shuffleFinishTime: Long = 0L
    var remoteBlocksFetched: Int = 0
    var localBlocksFetched: Int = 0
    var fetchWaitTime: Long = 0L
    var remoteBytesRead: Long = 0L

    // Shuffle write metrics.
    var hasShuffleWriteMetrics = false
    var shuffleBytesWritten: Long = 0L
    var shuffleWriteTime: Long = 0L

    taskMetrics.filter(filter).foreach { case (_, _, _, m) =>
      executorDeserializeTime += m.executorDeserializeTime
      executorRunTime += m.executorRunTime
      resultSize += m.resultSize
      jvmGCTime += m.jvmGCTime
      resultSerializationTime += m.resultSerializationTime
      memoryBytesSpilled += m.memoryBytesSpilled
      diskBytesSpilled += m.diskBytesSpilled

      m.inputMetrics.foreach { im =>
        hasInputMetrics = true
        val taskReadMethod = DataReadMethod.withName(im.readMethod.toString)
        if (readMethod == null) {
          readMethod = taskReadMethod
        } else if (readMethod != taskReadMethod) {
          readMethod = DataReadMethod.Multiple
        }
        bytesRead += im.bytesRead
      }

      m.shuffleReadMetrics.foreach { srm =>
        hasShuffleReadMetrics = true
        shuffleFinishTime = math.max(shuffleFinishTime, srm.shuffleFinishTime)
        remoteBlocksFetched += srm.remoteBlocksFetched
        localBlocksFetched += srm.localBlocksFetched
        fetchWaitTime += srm.fetchWaitTime
        remoteBytesRead += srm.remoteBytesRead
      }

      m.shuffleWriteMetrics.foreach { swm =>
        hasShuffleWriteMetrics = true
        shuffleBytesWritten += swm.shuffleBytesWritten
        shuffleWriteTime += swm.shuffleWriteTime
      }
    }

    val inputMetrics: Option[InputMetrics] =
      if (hasInputMetrics) {
        Some(new InputMetrics(readMethod, bytesRead))
      } else {
        None
      }

    val shuffleReadMetrics: Option[ShuffleReadMetrics] =
      if (hasShuffleReadMetrics) {
        Some(new ShuffleReadMetrics(shuffleFinishTime, remoteBlocksFetched, localBlocksFetched,
          fetchWaitTime, remoteBytesRead))
      } else {
        None
      }

    val shuffleWriteMetrics: Option[ShuffleWriteMetrics] =
      if (hasShuffleReadMetrics) {
        Some(new ShuffleWriteMetrics(shuffleBytesWritten, shuffleWriteTime))
      } else {
        None
      }

    new Metrics(
        executorDeserializeTime,
        executorRunTime,
        resultSize,
        jvmGCTime,
        resultSerializationTime,
        memoryBytesSpilled,
        diskBytesSpilled,
        inputMetrics,
        shuffleReadMetrics,
        shuffleWriteMetrics)
  }

  private def read[T](f: () => T): T = {
    lock.readLock.lock()
    try {
      f()
    } finally {
      lock.readLock().unlock()
    }
  }

  private def write[T](f: () => T): T = {
    lock.writeLock().lock()
    try {
      f()
    } finally {
      lock.writeLock().unlock()
    }
  }

}
