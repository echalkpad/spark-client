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
import sbt._
import sbt.Keys._

object SparkClient extends Build {

  val sparkVersion = sys.props.get("spark.version").getOrElse("1.1.0")

  def projectSettings = Defaults.defaultSettings ++ Seq(
    organization        := "com.cloudera.spark",
    name                := "spark-client",
    version             := "0.1-SNAPSHOT",
    scalaVersion        := "2.10.4",
    fork in Test        := true,
    javaOptions in Test :=
      Seq("-Dspark.test.home=" + sys.props.get("spark.test.home").getOrElse("")),
    libraryDependencies ++= Seq(
      "org.apache.spark"      % "spark-core_2.10"   % sparkVersion,
      "com.novocode"          % "junit-interface"   % "0.9"         % "test",
      "junit"                 % "junit"             % "4.10"        % "test",
      "org.scalatest"         % "scalatest_2.10"    % "2.1.5"       % "test"
      )
    )

  lazy val root = Project("root", file("."), settings = projectSettings)

}
