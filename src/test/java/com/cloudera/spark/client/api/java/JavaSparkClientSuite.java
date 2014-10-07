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

package com.cloudera.spark.client.api.java;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.spark.api.java.JavaRDD;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import com.cloudera.spark.client.impl.ClientUtils;

public class JavaSparkClientSuite {

  // Timeouts are bad... mmmkay.
  private static final long TIMEOUT = 10;

  private Map<String, String> createConf(boolean local) {
    Map<String, String> conf = new HashMap<String, String>();
    if (local) {
      conf.put(ClientUtils.CONF_KEY_IN_PROCESS(), "true");
      conf.put("spark.master", "local");
      conf.put("spark.app.name", "JavaSparkClientSuite Local App");
    } else {
      String classpath = System.getProperty("java.class.path");
      conf.put("spark.master", "local");
      conf.put("spark.app.name", "SparkClientSuite Remote App");
      conf.put("spark.driver.extraClassPath", classpath);
      conf.put("spark.executor.extraClassPath", classpath);
    }
    return conf;
  }

  @Test
  public void testJobSubmission() throws Exception {
    runTest(true, new TestFunction() {
      @Override
      public void call(JavaSparkClient client) throws Exception {
        JavaJobHandle<String> handle = client.submit(new SimpleJob());
        assertEquals("hello", handle.get(TIMEOUT, TimeUnit.SECONDS));
      }
    });
  }

  @Test
  public void testSimpleSparkJob() throws Exception {
    runTest(true, new TestFunction() {
      @Override
      public void call(JavaSparkClient client) throws Exception {
        JavaJobHandle<Long> handle = client.submit(new SparkJob());
        assertEquals(Long.valueOf(5L), handle.get(TIMEOUT, TimeUnit.SECONDS));
      }
    });
  }

  @Test
  public void testErrorJob() throws Exception {
    runTest(true, new TestFunction() {
      @Override
      public void call(JavaSparkClient client) throws Exception {
      JavaJobHandle<String> handle = client.submit(new SimpleJob());
        try {
          handle.get(TIMEOUT, TimeUnit.SECONDS);
        } catch (ExecutionException ee) {
          assertTrue(ee.getCause() instanceof IllegalStateException);
        }
      }
    });
  }

  @Test
  public void testRemoteClient() throws Exception {
    runTest(false, new TestFunction() {
      @Override
      public void call(JavaSparkClient client) throws Exception {
        JavaJobHandle<String> handle = client.submit(new SimpleJob());
        assertEquals("hello", handle.get(TIMEOUT, TimeUnit.SECONDS));
      }
    });
  }

  private void runTest(boolean local, TestFunction test) throws Exception {
    Map<String, String> conf = createConf(local);
    JavaSparkClient.initialize(conf);
    JavaSparkClient client = null;
    try {
      client = JavaSparkClient.createClient(conf);
      test.call(client);
    } finally {
      if (client != null) {
        client.stop();
      }
      JavaSparkClient.uninitialize();
    }
  }

  private static class SimpleJob implements JavaJob<String> {

    @Override
    public String call(JavaJobContext jc) {
      return "hello";
    }

  }

  private static class SparkJob implements JavaJob<Long> {

    @Override
    public Long call(JavaJobContext jc) {
      JavaRDD<Integer> rdd = jc.sc().parallelize(Arrays.asList(1, 2, 3, 4, 5));
      return rdd.count();
    }

  }

  private static class ErrorJob implements JavaJob<String> {

    @Override
    public String call(JavaJobContext jc) {
      throw new IllegalStateException("This job does not work.");
    }

  }

  private static interface TestFunction {
    void call(JavaSparkClient client) throws Exception;
  }

}
