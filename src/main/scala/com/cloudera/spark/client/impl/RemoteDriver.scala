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

import java.util.concurrent.{ConcurrentHashMap, Executors, ExecutorService, Future}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.util.Try

import akka.actor.{Actor, ActorRef, ActorSelection, ActorSystem, Props}
import akka.remote.DisassociatedEvent
import org.apache.spark.{FutureAction, Logging, SparkConf, SparkContext}
import org.apache.spark.scheduler._

import com.cloudera.spark.client.Metrics
import com.cloudera.spark.client.impl.Protocol._

/**
 * Driver code for the Spark client library.
 */
private class RemoteDriver extends Logging {

  private val activeJobs = new ConcurrentHashMap[String, JobWrapper[_]]()
  private val shutdownLock = new Object()
  private var executor: ExecutorService = _
  private var jc: JobContextImpl = _

  private var system: ActorSystem = _
  private var actor: ActorRef = _
  private var client: ActorSelection = _
  private var running = true

  def run(args: Array[String]): Unit = {

    val (conf, remote) = parseArgs(args)

    executor = Executors.newCachedThreadPool()

    logInfo(s"Connecting to: $remote")
    val (actorSystem, akkaUrl) = ClientUtils.createActorSystem(Map(conf.getAll: _*))
    system = actorSystem
    actor = actorSystem.actorOf(Props(classOf[ServerActor], this), "RemoteDriver")
    client = actorSystem.actorSelection(remote)

    try {
      val sc = new SparkContext(conf)
      sc.addSparkListener(new ClientListener())
      jc = new JobContextImpl(sc)
    } catch {
      case e: Exception =>
        logError("Failed to start SparkContext.", e)
        shutdown(Error(e))
        if (!conf.contains(ClientUtils.CONF_KEY_IN_PROCESS)) {
          System.exit(1)
        }
    }

    client ! Hello(s"$akkaUrl/RemoteDriver")
    shutdownLock.synchronized {
      while (running) {
        shutdownLock.wait()
      }
    }
    executor.shutdownNow()
  }

  def shutdown(msg: Any = null) = synchronized {
    if (running) {
      logInfo("Shutting down remote driver.")
      running = false
      activeJobs.values().foreach { _.future.cancel(true) }
      if (msg != null) {
        client ! msg
      }
      if (jc != null) {
        jc.stop()
      }
      system.shutdown()
      shutdownLock.synchronized {
        shutdownLock.notifyAll()
      }
    }
  }

  private def parseArgs(cmdArgs: Array[String]) = {
    val conf = new SparkConf()
    var remote: String = null
    var args = cmdArgs.toList

    while (!args.isEmpty) {
      args match {
        case "--remote" :: value :: tail =>
          remote = value
          args = tail
        case "--secret" :: value :: tail =>
          conf.set(ClientUtils.CONF_KEY_SECRET, value)
          args = tail
        case "--conf" :: key :: value :: tail =>
          conf.set(key, value)
          args = tail
        case Nil =>
        case _ =>
          throw new IllegalArgumentException("Invalid command line: " + cmdArgs.mkString(" "))
      }
    }

    (conf, remote)
  }

  private class ServerActor extends Actor {

    override def receive = {
      case CancelJob(jobId) =>
        val job = activeJobs.get(jobId)
        if (job == null || !job.future.cancel(true)) {
          logInfo("Requested to cancel an already finished job.")
        }

      case DisassociatedEvent =>
        logDebug("Shutting down due to DisassociatedEvent.")
        shutdown()

      case EndSession =>
        logDebug("Shutting down due to EndSession request.")
        shutdown()

      case req @ JobRequest(jobId, job) =>
        logInfo(s"Received job request $jobId")
        val wrapper = new JobWrapper(req)
        activeJobs.put(jobId, wrapper)
        wrapper.submit()
    }

  }

  private class JobWrapper[T >: Serializable](req: JobRequest[T]) extends Runnable {

    val jobs = mutable.ListBuffer[FutureAction[_]]()
    val completed = new AtomicInteger()
    var future: Future[_] = _

    override def run() = {
      try {
        jc.setMonitorCb(monitorJob)
        val result = Try(req.job(jc))
        completed.synchronized {
          // TODO: Due to SPARK-3446, we can't test for equality here, since we don't know which
          // jobs for which we get SparkListenerJobEnd events are actually monitored.
          while (completed.get() < jobs.size) {
            logDebug(s"Client job ${req.id} finished, ${completed.get()} of ${jobs.size} Spark " +
              "jobs finished.")
            completed.wait()
          }
        }
        client ! JobResult(req.id, result)
      } finally {
        jc.setMonitorCb(null)
        activeJobs.remove(req.id)
      }
    }

    def submit() {
      this.future = executor.submit(this)
    }

    def jobDone() = completed.synchronized {
      completed.incrementAndGet()
      completed.notifyAll()
    }

    private def monitorJob(job: FutureAction[_]) = {
      jobs += job
    }

  }

  private class ClientListener extends SparkListener {

    private val stageToJobId = new mutable.HashMap[Int, Int]()

    override def onJobStart(jobStart: SparkListenerJobStart) = {
      // TODO: are stage IDs unique? Otherwise this won't work.
      stageToJobId.synchronized {
        jobStart.stageIds.foreach { i => stageToJobId += (i -> jobStart.jobId) }
      }
    }

    override def onJobEnd(jobEnd: SparkListenerJobEnd) = {
      stageToJobId.synchronized {
        val stageIds = stageToJobId
          .filter { case (k, v) => v == jobEnd.jobId }
          .map { case (k, v) => k }
        stageToJobId --= stageIds
      }

      getClientId(jobEnd.jobId).foreach { id => activeJobs.get(id).jobDone() }
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd) = {
      if (taskEnd.reason == org.apache.spark.Success && !taskEnd.taskInfo.speculative) {
        val metrics = new Metrics(taskEnd.taskMetrics)
        val jobId = stageToJobId.synchronized {
          stageToJobId(taskEnd.stageId)
        }

        // TODO: implement implicit AsyncRDDActions conversion instead of jc.monitor()?
        // TODO: how to handle stage failures?

        getClientId(jobId).foreach { id =>
          client ! JobMetrics(id, jobId, taskEnd.stageId, taskEnd.taskInfo.taskId, metrics)
        }
      }
    }

    /**
     * Returns the client job ID for the given Spark job ID.
     *
     * This will only work for jobs monitored via JobContext#monitor(). Other jobs won't be
     * matched, and this method will return `None`.
     *
     * HACK NOTE: due to SPARK-3446 it's not currently possible to do this match. So, at the
     * moment, this is a huge hack.
     */
    private def getClientId(jobId: Int): Option[String] = {
      Option(Try(activeJobs.keySet().iterator().next()).getOrElse(null))
    }

  }

}

private[impl] object RemoteDriver extends Logging {

  def main(args: Array[String]): Unit = {
    new RemoteDriver().run(args)
  }

}
