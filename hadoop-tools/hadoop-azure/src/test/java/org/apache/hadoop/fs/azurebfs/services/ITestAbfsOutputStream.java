/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.net.URI;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;

/**
 * Test create operation.
 */
public class ITestAbfsOutputStream extends AbstractAbfsIntegrationTest {
  private static final String TEST_FILE_PATH = "testfile";

  public ITestAbfsOutputStream() throws Exception {
    super();
  }

  @Test
  public void testMaxRequestsAndQueueCapacityDefaults() throws Exception {
    Configuration conf = getRawConfiguration();
    final AzureBlobFileSystem fs = getFileSystem(conf);
    try (FSDataOutputStream out = fs.create(path(TEST_FILE_PATH))) {
    AbfsOutputStream stream = (AbfsOutputStream) out.getWrappedStream();

      int maxConcurrentRequests
          = getConfiguration().getWriteMaxConcurrentRequestCount();
      if (stream.isAppendBlobStream()) {
        maxConcurrentRequests = 1;
      }

    Assertions.assertThat(stream.getMaxConcurrentRequestCount()).describedAs(
        "maxConcurrentRequests should be " + maxConcurrentRequests)
        .isEqualTo(maxConcurrentRequests);
    Assertions.assertThat(stream.getMaxRequestsThatCanBeQueued()).describedAs(
        "maxRequestsToQueue should be " + getConfiguration()
            .getMaxWriteRequestsToQueue())
        .isEqualTo(getConfiguration().getMaxWriteRequestsToQueue());
    }
  }

  @Test
  public void testMaxRequestsAndQueueCapacity() throws Exception {
    Configuration conf = getRawConfiguration();
    int maxConcurrentRequests = 6;
    int maxRequestsToQueue = 10;
    conf.set(ConfigurationKeys.AZURE_WRITE_MAX_CONCURRENT_REQUESTS,
        "" + maxConcurrentRequests);
    conf.set(ConfigurationKeys.AZURE_WRITE_MAX_REQUESTS_TO_QUEUE,
        "" + maxRequestsToQueue);
    final AzureBlobFileSystem fs = getFileSystem(conf);
    try (FSDataOutputStream out = fs.create(path(TEST_FILE_PATH))) {
      AbfsOutputStream stream = (AbfsOutputStream) out.getWrappedStream();

      if (stream.isAppendBlobStream()) {
        maxConcurrentRequests = 1;
      }

      Assertions.assertThat(stream.getMaxConcurrentRequestCount()).describedAs(
          "maxConcurrentRequests should be " + maxConcurrentRequests).isEqualTo(maxConcurrentRequests);
      Assertions.assertThat(stream.getMaxRequestsThatCanBeQueued()).describedAs("maxRequestsToQueue should be " + maxRequestsToQueue)
          .isEqualTo(maxRequestsToQueue);
    }
  }

  /**
   * Verify the passing of AzureBlobFileSystem reference to AbfsOutputStream
   * to make sure that the FS instance is not eligible for GC.
   *
   */
  @Test
  public void testAzureBlobFileSystemBackReferenceInOutputStream()
      throws Exception {
    AzureBlobFileSystem fs1 = new AzureBlobFileSystem();
    fs1.initialize(new URI(getTestUrl()), getRawConfiguration());
    Path pathFs1 = path(getMethodName() + "1");

    AzureBlobFileSystem fs2 = new AzureBlobFileSystem();
    fs2.initialize(new URI(getTestUrl()), getRawConfiguration());
    Path pathFs2 = path(getMethodName() + "2");

    try(AbfsOutputStream out1 = createAbfsOutputStreamWithFlushEnabled(fs1,
        pathFs1)) {
      Assert.assertEquals("Mismatch in Filesystem reference this outputStream"
              + " should have",
          fs1, out1.getFsBackRef());
    }

    try(AbfsOutputStream out2 = createAbfsOutputStreamWithFlushEnabled(fs2,
        pathFs2)) {
      Assert.assertEquals("Mismatch in Filesystem reference this outputStream"
          + " should have", fs2, out2.getFsBackRef());
    }
  }

}
