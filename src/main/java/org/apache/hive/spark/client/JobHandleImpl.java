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

package org.apache.hive.spark.client;

import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

/**
 * A handle to a submitted job. Allows for monitoring and controlling of the running remote job.
 */
class JobHandleImpl<T extends Serializable> implements JobHandle<T> {

  private final SparkClientImpl client;
  private final String jobId;
  private final MetricsCollection metrics;
  private final Object monitor;

  private boolean cancelled;
  private boolean completed;
  private T result;
  private Throwable error;

  JobHandleImpl(SparkClientImpl client, String jobId) {
    this.client = client;
    this.jobId = jobId;
    this.monitor = new Object();
    this.metrics = new MetricsCollection();
  }

  /** Requests a running job to be cancelled. */
  @Override
  public boolean cancel(boolean unused) {
    client.cancel(jobId);
    return true;
  }

 	@Override
 	public T get() throws ExecutionException, InterruptedException {
 	  try {
 	    return get(-1);
 	  } catch (TimeoutException te) {
 	    // Shouldn't really happen.
 	    throw new ExecutionException(te);
 	  }
 	}

 	@Override
 	public T get(long timeout, TimeUnit unit)
 	    throws ExecutionException, InterruptedException, TimeoutException {
 	  return get(unit.toMillis(timeout));
  }

 	@Override
 	public boolean isCancelled() {
 	  return cancelled;
 	}

 	@Override
 	public boolean isDone() {
 	  return completed;
 	}

  /**
   * The client job ID. This is unrelated to any Spark jobs that might be triggered by the
   * submitted job.
   */
  @Override
  public String getClientJobId() {
    return jobId;
  }

  /**
   * A collection of metrics collected from the Spark jobs triggered by this job.
   *
   * To collect job metrics on the client, Spark jobs must be registered with JobContext::monitor()
   * on the remote end.
   */
  @Override
  public MetricsCollection getMetrics() {
    return metrics;
  }

  private T get(long timeout) throws ExecutionException, InterruptedException, TimeoutException {
    long deadline = System.currentTimeMillis() + timeout;
    synchronized (monitor) {
      while (!completed && !cancelled) {
        if (timeout >= 0) {
          monitor.wait(timeout);
        } else {
          monitor.wait();
        }
        if (timeout >= 0 && System.currentTimeMillis() >= deadline) {
          throw new TimeoutException();
        }
      }
    }

    if (error != null) {
      throw new ExecutionException(error);
    }

    return result;
  }

  // TODO: expose job status?

  @SuppressWarnings("unchecked")
  void complete(Object result, Throwable error) {
    if (result != null && error != null) {
      throw new IllegalArgumentException("Either result or error should be set.");
    }
    synchronized (monitor) {
      this.result = (T) result;
      this.error = error;
      this.completed = true;
      monitor.notifyAll();
    }
  }

}
