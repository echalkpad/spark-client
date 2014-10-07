
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

import java.io.{File, FileOutputStream, InputStream, IOException, OutputStream, OutputStreamWriter}
import java.net.URL
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.concurrent.Future

import akka.actor.{Actor, ActorRef, ActorSelection, ActorSystem, Props}
import org.apache.spark.{Logging, SparkContext, SparkException}
import org.apache.spark.deploy.SparkSubmit

import com.cloudera.spark.client._
import com.cloudera.spark.client.impl.Protocol._

/**
 * Defines the client interface for a remote Spark context.
 */
private[client] class SparkClientImpl(conf: Map[String, String]) extends SparkClient
  with Logging {

  import SparkClientImpl._

  private val childIdGenerator = new AtomicInteger()
  private val name = "SparkClient-" + ClientUtils.randomName()
  private val clientRef = bind(Props(classOf[ClientActor], this), name)
  private val driverThread = startDriver()

  private var remoteRef: ActorSelection = _
  this.synchronized {
    val connectTimeout = conf.get("spark.client.connectTimeout").getOrElse("10").toInt * 1000
    val endTime = System.currentTimeMillis() + connectTimeout
    while (remoteRef == null) {
      wait(connectTimeout)
      if (remoteRef == null && System.currentTimeMillis() > endTime) {
        throw new SparkException("Timed out waiting for remote driver to connect.")
      }
    }
  }

  private val jobs = new ConcurrentHashMap[String, JobHandleImpl[_]]()

  override def submit[T >: Serializable](job: JobContext => T): JobHandle[T] = {
    val jobId = ClientUtils.randomName()
    remoteRef ! JobRequest(jobId, job)

    val handle = new JobHandleImpl[T](this, jobId)
    jobs.put(jobId, handle)
    handle
  }

  override def stop(): Unit = {
    if (remoteRef != null) {
      logInfo("Sending EndSession to remote actor.")
      remoteRef ! EndSession
    }
    unbind(clientRef)
    driverThread.join() // TODO: timeout?
  }

  override def addJar(url: URL): Future[_] = {
    val path = url.toString()
    submit(jc => jc.sc.addJar(path))
  }

  override def addFile(url: URL): Future[_] = {
    val path = url.toString()
    submit(jc => jc.sc.addFile(path))
  }

  private[impl] def cancel(jobId: String) = {
    remoteRef ! CancelJob(jobId)
  }

  private def startDriver(): Thread = {
    val runnable =
      if (conf.contains(ClientUtils.CONF_KEY_IN_PROCESS)) {
        // Mostly for testing things quickly. Do not do this in production.
        logWarning("!!!! Running remote driver in-process. !!!!")
        new Runnable() {
          override def run() {
            val args = Seq(
              "--remote", s"$akkaUrl/$name",
              "--secret", secret) ++
              conf.flatMap { case (k, v) => Seq("--conf", k, v) }
            RemoteDriver.main(args.toArray)
          }
        }
      } else {
        // Create a file with all the job properties to be read by spark-submit. Change the
        // file's permissions so that only the owner can read it. This avoid having the
        // connection secret show up in the child process's command line.
        val properties = File.createTempFile("spark-submit.", ".properties")
        if (!properties.setReadable(false) || !properties.setReadable(true, true)) {
          throw new IOException("Cannot change permissions of job properties file.")
        }

        val allProps = new Properties()
        conf.foreach { case (k, v) => allProps.put(k, v) }
        allProps.put(ClientUtils.CONF_KEY_SECRET, secret)

        val writer = new OutputStreamWriter(new FileOutputStream(properties), "UTF-8")
        try {
          allProps.store(writer, "Spark Context configuration")
        } finally {
          writer.close()
        }

        // Define how to pass options to the child process. If launching in client (or local)
        // mode, the driver options need to be passed directly on the command line. Otherwise,
        // SparkSubmit will take care of that for us.
        val master = conf.get("spark.master").getOrElse(
          throw new IllegalArgumentException("spark.master is not defined."))
        val jvmOpts: Seq[String] =
          if (master.startsWith("local") || master.startsWith("mesos") || master.endsWith("-client")) {
            val opts = new mutable.ListBuffer[String]()

            conf.get("spark.driver.memory").foreach { x =>
              opts += s"-Xms$x"
              opts += s"-Xmx$x"
            }
            conf.get("spark.driver.extraClassPath").foreach { x =>
              opts += "-classpath"
              opts += x
            }
            conf.get("spark.driver.extraLibPath").foreach { x =>
              opts += s"-Djava.library.path=$x"
            }
            conf.get("spark.driver.extraJavaOptions").foreach { x =>
              opts ++= x.split(" ")
            }

            opts
          } else {
            Nil
          }

        val java = new File(sys.props("java.home"), "bin/java").getAbsolutePath()
        val argv = Seq(java) ++ jvmOpts ++ Array(
          SparkSubmit.getClass.getName().stripSuffix("$"),
          "--properties-file", properties.getAbsolutePath(),
          "--class", RemoteDriver.getClass.getName().stripSuffix("$"),
          SparkContext.jarOfClass(this.getClass()).getOrElse("spark-internal"),
          "--remote", s"$akkaUrl/$name"
          )
        logDebug(s"Running client driver with argv: ${argv.mkString(" ")}")

        val pb = new ProcessBuilder(argv: _*)
        pb.environment().clear()
        val child = pb.start()

        val childId = childIdGenerator.incrementAndGet()
        redirect(s"stdout-redir-$childId", child.getInputStream(), System.out)
        redirect(s"stderr-redir-$childId", child.getErrorStream(), System.err)

        new Runnable() {
          override def run() {
            try {
              val exitCode = child.waitFor()
              if (exitCode != 0) {
                logWarning(s"Child process exited with code $exitCode.")
              }
            } catch {
              case e: Exception =>
                logWarning("Exception while waiting for child process.", e)
            }
          }
        }
      }

    val thread = new Thread(runnable)
    thread.setDaemon(true)
    thread.setName("Driver")
    thread.start()
    thread
  }

  private def redirect(name: String, in: InputStream, out: OutputStream) = {
    val thread = new Thread(new Redirector(in, out))
    thread.setName(name)
    thread.setDaemon(true)
    thread.start()
  }

  private class ClientActor extends Actor {

    override def receive = {
      case Error(e) =>
        logError("Error report from remote driver.", e)

      case Hello(remoteUrl) =>
        logInfo(s"Received hello from $remoteUrl")
        remoteRef = select(remoteUrl)
        SparkClientImpl.this.synchronized {
          SparkClientImpl.this.notifyAll()
        }

      case JobMetrics(jobId, sparkJob, stage, task, metrics) =>
        val handle = jobs.get(jobId)
        if (handle != null) {
          handle.metrics.addMetrics(sparkJob, stage, task, metrics)
        } else {
          logWarning(s"Received metrics for unknown job $jobId")
        }

      case JobResult(jobId, result) =>
        val handle = jobs.remove(jobId)
        if (handle != null) {
          logInfo(s"Received result for $jobId")
          handle.complete(result)
        } else {
          logWarning(s"Received result for unknown job $jobId")
        }
    }

  }

  private class Redirector(in: InputStream, out: OutputStream) extends Runnable {

    override def run() {
      val buf = new Array[Byte](1024)
      var len = in.read(buf)
      while (len != -1) {
        out.write(buf, 0, len)
        out.flush()
        len = in.read(buf)
      }
    }

  }

}

/**
 * Holds global state for the SparkClient. Mainly, the Akka actor system that is shared among
 * all client instances.
 */
private[client] object SparkClientImpl extends Logging {
  private var actorSystem: ActorSystem = _
  private var akkaUrl: String = _
  private var secret: String = _

  def initialize(conf: Map[String, String]): Unit = {
    this.secret = akka.util.Crypt.generateSecureCookie
    val akkaConf = conf + (ClientUtils.CONF_KEY_SECRET -> secret)
    val (system, url) = ClientUtils.createActorSystem(akkaConf)
    this.actorSystem = system
    this.akkaUrl = url
  }

  def stop(): Unit = {
    if (actorSystem != null) {
      actorSystem.shutdown()
      actorSystem = null
    }
  }

  private def bind(props: Props, name: String): ActorRef = actorSystem.actorOf(props, name)

  private def unbind(actor: ActorRef) = actorSystem.stop(actor)

  private def select(url: String) = actorSystem.actorSelection(url)

}
