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

package org.apache.spark_remote.api.java

import java.util.concurrent.{ExecutionException, Future, TimeoutException}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util._

import org.apache.spark_remote._

/**
 * A handle to a submitted job. Allows for monitoring and controlling of the running remote job.
 */
class JavaJobHandle[T >: Serializable] private[java] (private val handle: JobHandle[T])
  extends Future[T] {

  private var cancelled = false

  /** Requests a running job to be cancelled. */
  override def cancel(unused: Boolean): Boolean = {
    handle.cancel()
    cancelled = true
    cancelled
  }

  @throws[ExecutionException]
  @throws[InterruptedException]
  @throws[TimeoutException]
 	override def get(): T = {
 	  get(Duration.Inf)
 	}

  @throws[ExecutionException]
  @throws[InterruptedException]
  @throws[TimeoutException]
 	override def get(timeout: Long, unit: TimeUnit) = {
    get(Duration(timeout, unit))
  }

 	override def isCancelled() = cancelled

  override def isDone() = handle.isCompleted

  private def get(timeout: Duration) = {
    // Should either complete or throw an exception.
    Await.ready(handle, timeout)

    handle.value.map {
      _ match {
        case Success(value) => value.asInstanceOf[T]
        case Failure(err) => throw new ExecutionException(err)
      }
    }.get
  }

  // TODO: expose metrics, job status?

}
