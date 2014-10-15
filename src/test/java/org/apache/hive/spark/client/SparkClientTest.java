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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.jar.JarOutputStream;

import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import org.apache.spark.FutureAction;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class SparkClientTest {

  // Timeouts are bad... mmmkay.
  private static final long TIMEOUT = 10;

  private Map<String, String> createConf(boolean local) {
    Map<String, String> conf = new HashMap<String, String>();
    if (local) {
      conf.put(ClientUtils.CONF_KEY_IN_PROCESS, "true");
      conf.put("spark.master", "local");
      conf.put("spark.app.name", "SparkClientSuite Local App");
    } else {
      String classpath = System.getProperty("java.class.path");
      conf.put("spark.master", "local");
      conf.put("spark.app.name", "SparkClientSuite Remote App");
      conf.put("spark.driver.extraClassPath", classpath);
      conf.put("spark.executor.extraClassPath", classpath);
    }

    if (!Strings.isNullOrEmpty(System.getProperty("spark.home"))) {
      conf.put("spark.home", System.getProperty("spark.home"));
    }

    return conf;
  }

  @Test
  public void testJobSubmission() throws Exception {
    runTest(true, new TestFunction() {
      @Override
      public void call(SparkClient client) throws Exception {
        JobHandle<String> handle = client.submit(new SimpleJob());
        assertEquals("hello", handle.get(TIMEOUT, TimeUnit.SECONDS));
      }
    });
  }

  @Test
  public void testSimpleSparkJob() throws Exception {
    runTest(true, new TestFunction() {
      @Override
      public void call(SparkClient client) throws Exception {
        JobHandle<Long> handle = client.submit(new SparkJob());
        assertEquals(Long.valueOf(5L), handle.get(TIMEOUT, TimeUnit.SECONDS));
      }
    });
  }

  @Test
  public void testErrorJob() throws Exception {
    runTest(true, new TestFunction() {
      @Override
      public void call(SparkClient client) throws Exception {
      JobHandle<String> handle = client.submit(new SimpleJob());
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
      public void call(SparkClient client) throws Exception {
        JobHandle<Long> handle = client.submit(new SparkJob());
        assertEquals(Long.valueOf(5L), handle.get(TIMEOUT, TimeUnit.SECONDS));
      }
    });
  }

  @Test
  public void testMetricsCollection() throws Exception {
    runTest(true, new TestFunction() {
      @Override
      public void call(SparkClient client) throws Exception {
        JobHandle<Integer> future = client.submit(new AsyncSparkJob());
        future.get(TIMEOUT, TimeUnit.SECONDS);
        MetricsCollection metrics = future.getMetrics();
        assertEquals(1, metrics.getJobIds().size());
        assertTrue(metrics.getAllMetrics().executorRunTime > 0L);

        JobHandle<Integer> future2 = client.submit(new AsyncSparkJob());
        future2.get(TIMEOUT, TimeUnit.SECONDS);
        MetricsCollection metrics2 = future2.getMetrics();
        assertEquals(1, metrics2.getJobIds().size());
        assertNotEquals(metrics.getJobIds(), metrics2.getJobIds());
        assertTrue(metrics2.getAllMetrics().executorRunTime > 0L);
      }
    });
  }

  @Test
  public void testAddJarsAndFiles() throws Exception {
    runTest(true, new TestFunction() {
      @Override
      public void call(SparkClient client) throws Exception {
        File jar = null;
        File file = null;

        try {
          // Test that adding a jar to the remote context makes it show up in the classpath.
          jar = File.createTempFile("test", ".jar");

          JarOutputStream jarFile = new JarOutputStream(new FileOutputStream(jar));
          jarFile.putNextEntry(new ZipEntry("test.resource"));
          jarFile.write("test resource".getBytes("UTF-8"));
          jarFile.closeEntry();
          jarFile.close();

          client.addJar(new URL("file:" + jar.getAbsolutePath()))
            .get(TIMEOUT, TimeUnit.SECONDS);

          // Need to run a Spark job to make sure the jar is added to the class loader. Monitoring
          // SparkContext#addJar() doesn't mean much, we can only be sure jars have been distributed
          // when we run a task after the jar has been added.
          String result = client.submit(new JarJob()).get(TIMEOUT, TimeUnit.SECONDS);
          assertEquals("test resource", result);

          // Test that adding a file to the remote context makes it available to executors.
          file = File.createTempFile("test", ".file");

          FileOutputStream fileStream = new FileOutputStream(file);
          fileStream.write("test file".getBytes("UTF-8"));
          fileStream.close();

          client.addJar(new URL("file:" + file.getAbsolutePath()))
            .get(TIMEOUT, TimeUnit.SECONDS);

          // The same applies to files added with "addFile". They're only guaranteed to be available
          // to tasks started after the addFile() call completes.
          result = client.submit(new FileJob(file.getName()))
            .get(TIMEOUT, TimeUnit.SECONDS);
          assertEquals("test file", result);
        } finally {
          if (jar != null) {
            jar.delete();
          }
          if (file != null) {
            file.delete();
          }
        }
      }
    });
  }

  @Test
  public void testKryoSerializer() throws Exception {
    runTest(true, new TestFunction() {
      @Override
      public void call(SparkClient client) throws Exception {
        JobHandle<Long> handle = client.submit(new SparkJob());
        assertEquals(Long.valueOf(5L), handle.get(TIMEOUT, TimeUnit.SECONDS));
      }

      @Override void config(Map<String, String> conf) {
        conf.put(ClientUtils.CONF_KEY_SERIALIZER, "kryo");
      }
    });
  }


  private void runTest(boolean local, TestFunction test) throws Exception {
    Map<String, String> conf = createConf(local);
    SparkClientFactory.initialize(conf);
    SparkClient client = null;
    try {
      test.config(conf);
      client = SparkClientFactory.createClient(conf);
      test.call(client);
    } finally {
      if (client != null) {
        client.stop();
      }
      SparkClientFactory.stop();
    }
  }

  private static class SimpleJob implements Job<String> {

    @Override
    public String call(JobContext jc) {
      return "hello";
    }

  }

  private static class SparkJob implements Job<Long> {

    @Override
    public Long call(JobContext jc) {
      JavaRDD<Integer> rdd = jc.sc().parallelize(Arrays.asList(1, 2, 3, 4, 5));
      return rdd.count();
    }

  }

  private static class AsyncSparkJob implements Job<Integer> {

    @Override
    public Integer call(JobContext jc) throws Exception {
      JavaRDD<Integer> rdd = jc.sc().parallelize(Arrays.asList(1, 2, 3, 4, 5));

      // XXX: Scala API is not friendly to Java code...
      FutureAction<?> future = jc.monitor(rdd.foreachAsync(new VoidFunction<Integer>() {
        @Override
        public void call(Integer l) throws Exception {

        }
      }));

      long deadline = System.currentTimeMillis() + TIMEOUT * 1000;
      while (!future.isCompleted()) {
        Thread.sleep(100);
        if (System.currentTimeMillis() > deadline) {
          fail("Timed out waiting for async job.");
        }
      }


      return 1;
    }

  }

  private static class ErrorJob implements Job<String> {

    @Override
    public String call(JobContext jc) {
      throw new IllegalStateException("This job does not work.");
    }

  }

  private static class JarJob implements Job<String>, Function<Integer, String> {

    @Override
    public String call(JobContext jc) {
      return jc.sc().parallelize(Arrays.asList(1)).map(this).collect().get(0);
    }

    @Override
    public String call(Integer i) throws Exception {
      ClassLoader ccl = Thread.currentThread().getContextClassLoader();
      InputStream in = ccl.getResourceAsStream("test.resource");
      byte[] bytes = ByteStreams.toByteArray(in);
      in.close();
      return new String(bytes, 0, bytes.length, "UTF-8");
    }

  }

  private static class FileJob implements Job<String>, Function<Integer, String> {

    private final String fileName;

    FileJob(String fileName) {
      this.fileName = fileName;
    }

    @Override
    public String call(JobContext jc) {
      return jc.sc().parallelize(Arrays.asList(1)).map(this).collect().get(0);
    }

    @Override
    public String call(Integer i) throws Exception {
      InputStream in = new FileInputStream(SparkFiles.get(fileName));
      byte[] bytes = ByteStreams.toByteArray(in);
      in.close();
      return new String(bytes, 0, bytes.length, "UTF-8");
    }

  }

  private static abstract class TestFunction {
    abstract void call(SparkClient client) throws Exception;
    void config(Map<String, String> conf) { }
  }

}
