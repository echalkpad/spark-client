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

import java.net.{InetAddress, Inet4Address, NetworkInterface}
import java.util.UUID

import scala.collection.JavaConversions._

import akka.actor.{ActorSystem, ExtendedActorSystem}
import com.typesafe.config.ConfigFactory
import org.apache.spark.Logging

import com.cloudera.spark.client._

private[client] object ClientUtils extends Logging {

  val CONF_KEY_SECRET = "spark.client.authentication.secret"
  val CONF_KEY_IN_PROCESS = "spark.client.do_not_use_this.run_driver_in_process"
  val CONF_KEY_SERIALIZER = "spark-remote.akka.serializer"

  /**
   * Create a new ActorSystem based on the given configuration.
   *
   * The akka configs are the same used to configure Akka in Spark.
   *
   * @param conf Configuration.
   * @return 2-tuple (actor system, akka root url)
   */
  def createActorSystem(conf: Map[String, String]): (ActorSystem, String) = {
    def getBoolean(key: String) =
      conf.get("spark.akka.logAkkaConfig").getOrElse("false").toBoolean

    def getInt(key: String, default: Int) =
      conf.get(key).map(_.toInt).getOrElse(default)

    def getDouble(key: String, default: Double) =
      conf.get(key).map(_.toDouble).getOrElse(default)

    val akkaThreads = getInt("spark.akka.threads", 4)
    val akkaBatchSize = getInt("spark.akka.batchSize", 15)
    val akkaTimeout = getInt("spark.akka.timeout", 100)
    val akkaFrameSize = getInt("spark.akka.frameSize", 10) * 1024 * 1024
    val lifecycleEvents = if (getBoolean("spark.akka.logLifecycleEvents")) "on" else "off"
    val logAkkaConfig = if (getBoolean("spark.akka.logAkkaConfig")) "on" else "off"

    val akkaHeartBeatPauses = getInt("spark.akka.heartbeat.pauses", 600)
    val akkaFailureDetector = getDouble("spark.akka.failure-detector.threshold", 300.0)
    val akkaHeartBeatInterval = getInt("spark.akka.heartbeat.interval", 1000)
    val akkaSerializer = conf.get(CONF_KEY_SERIALIZER).getOrElse("java")

    val secret = conf.get(CONF_KEY_SECRET).getOrElse(
      throw new IllegalArgumentException(s"$CONF_KEY_SECRET not set."))

    val host = findLocalIpAddress()


    val akkaConf = ConfigFactory.parseMap(conf.filter { case (k, v) => k.startsWith("akka.") })
      .withFallback(
        ConfigFactory.parseString(
        s"""
        |akka.daemonic = on
        |akka.loggers = [""akka.event.slf4j.Slf4jLogger""]
        |akka.stdout-loglevel = "ERROR"
        |akka.jvm-exit-on-fatal-error = off
        |akka.actor.default-dispatcher.throughput = $akkaBatchSize
        |akka.actor.serializers.java = "akka.serialization.JavaSerializer"
        |akka.actor.serializers.kryo = "com.twitter.chill.akka.AkkaSerializer"
        |akka.actor.serialization-bindings = { "java.io.Serializable" = "$akkaSerializer" }
        |akka.log-config-on-start = $logAkkaConfig
        |akka.log-dead-letters = $lifecycleEvents
        |akka.log-dead-letters-during-shutdown = $lifecycleEvents
        |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
        |akka.remote.log-remote-lifecycle-events = $lifecycleEvents
        |akka.remote.netty.tcp.connection-timeout = $akkaTimeout s
        |akka.remote.netty.tcp.execution-pool-size = $akkaThreads
        |akka.remote.netty.tcp.hostname = "$host"
        |akka.remote.netty.tcp.maximum-frame-size = ${akkaFrameSize}B
        |akka.remote.netty.tcp.port = 0
        |akka.remote.netty.tcp.tcp-nodelay = on
        |akka.remote.netty.tcp.transport-class = "akka.remote.transport.netty.NettyTransport"
        |akka.remote.require-cookie = "on"
        |akka.remote.secure-cookie = "$secret"
        |akka.remote.transport-failure-detector.acceptable-heartbeat-pause = $akkaHeartBeatPauses s
        |akka.remote.transport-failure-detector.heartbeat-interval = $akkaHeartBeatInterval s
        |akka.remote.transport-failure-detector.threshold = $akkaFailureDetector
        """.stripMargin))

    val name = randomName()
    val actorSystem = ActorSystem(name, akkaConf)
    val provider = actorSystem.asInstanceOf[ExtendedActorSystem].provider
    val boundPort = provider.getDefaultAddress.port.get
    (actorSystem, s"akka.tcp://$name@$host:$boundPort/user")
  }

  def randomName(): String = UUID.randomUUID().toString()

  // Copied from Utils.scala.
  private def findLocalIpAddress(): String = {
    sys.env.get("SPARK_LOCAL_IP").getOrElse {
      val address = InetAddress.getLocalHost
      if (address.isLoopbackAddress) {
        // Address resolves to something like 127.0.1.1, which happens on Debian; try to find
        // a better address using the local network interfaces
        for (ni <- NetworkInterface.getNetworkInterfaces) {
          for (addr <- ni.getInetAddresses if !addr.isLinkLocalAddress &&
               !addr.isLoopbackAddress && addr.isInstanceOf[Inet4Address]) {
            // We've found an address that looks reasonable!
            logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
              " a loopback address: " + address.getHostAddress + "; using " + addr.getHostAddress +
              " instead (on interface " + ni.getName + ")")
            logWarning("Set SPARK_LOCAL_IP if you need to bind to another address")
            return addr.getHostAddress
          }
        }
        logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
          " a loopback address: " + address.getHostAddress + ", but we couldn't find any" +
          " external IP address!")
        logWarning("Set SPARK_LOCAL_IP if you need to bind to another address")
      }
      address.getHostAddress
    }
  }

}
