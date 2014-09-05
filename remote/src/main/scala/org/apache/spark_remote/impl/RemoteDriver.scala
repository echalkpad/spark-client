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

package org.apache.spark_remote.impl

import java.util.concurrent.{ConcurrentHashMap, Executors, ExecutorService, Future}

import scala.collection.JavaConversions._
import scala.util.Try

import akka.actor.{Actor, ActorRef, ActorSelection, ActorSystem, Props}
import akka.remote.DisassociatedEvent

import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark_remote.impl.Protocol._

/**
 * Driver code for the Spark client library.
 */
private class RemoteDriver extends Logging {

  private val activeJobs = new ConcurrentHashMap[String, Future[_]]()
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
      activeJobs.values().foreach { _.cancel(true) }
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
        val job = activeJobs.remove(jobId)
        if (job == null || !job.cancel(true)) {
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
        activeJobs.put(jobId, executor.submit(new JobWrapper(req)))
    }

  }

  private class JobWrapper[T >: Serializable](req: JobRequest[T]) extends Runnable {

    override def run() = {
      try {
        val result = Try(req.job(jc))
        client ! JobResult(req.id, result)
      } finally {
        activeJobs.remove(req.id)
      }
    }

  }

}

private[impl] object RemoteDriver extends Logging {

  def main(args: Array[String]): Unit = {
    new RemoteDriver().run(args)
  }

}
