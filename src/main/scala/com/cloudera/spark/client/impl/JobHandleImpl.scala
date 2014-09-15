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

package com.cloudera.spark.client.impl

import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag
import scala.util._

import org.apache.spark.SparkConf

import com.cloudera.spark.client._

/**
 * A handle to a submitted job. Allows for monitoring and controlling of the running remote job.
 */
private[impl] class JobHandleImpl[T >: Serializable](client: SparkClientImpl, jobId: String)
  extends JobHandle[T] {

  private var result: Try[T] = null
  private var monitor = new Object()
  private var metricsCollection = new MetricsCollection()

  override def clientJobId = jobId

  override def metrics = metricsCollection

  override def isCompleted = result != null

  override def onComplete[U](f: (Try[T]) => U)(implicit executor: ExecutionContext): Unit = {
    executor.execute(new Runnable {
      override def run() {
        f(awaitResult())
      }
    })
  }

  override def ready(atMost: Duration)(implicit permit: CanAwait) = {
    if (!atMost.isFinite()) {
      awaitResult()
    } else monitor.synchronized {
      val finishTime = System.currentTimeMillis() + atMost.toMillis
      while (result == null) {
        val time = System.currentTimeMillis()
        if (time >= finishTime) {
          throw new TimeoutException
        } else {
          monitor.wait(finishTime - time)
        }
      }
    }
    this
  }

  override def result(atMost: Duration)(implicit permit: CanAwait) = {
    ready(atMost)(permit)
    awaitResult() match {
      case Success(res) => res
      case Failure(e) => throw e
    }
  }

  override def value: Option[Try[T]] = Option(result)

  def cancel(): Unit = {
    client.cancel(jobId)
  }

  private def awaitResult(): Try[T] = {
    monitor.synchronized {
      while (result == null) {
        monitor.wait()
      }
    }
    result
  }

  private[impl] def complete(result: Try[_]) = {
    monitor.synchronized {
      this.result = result.asInstanceOf[Try[T]]
      monitor.notifyAll()
    }
  }

}
