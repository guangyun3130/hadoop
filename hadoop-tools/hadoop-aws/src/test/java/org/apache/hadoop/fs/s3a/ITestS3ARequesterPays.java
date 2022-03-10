/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import java.nio.file.AccessDeniedException;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.IOStatisticAssertions;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Tests for Requester Pays feature.
 */
public class ITestS3ARequesterPays extends AbstractS3ATestBase {

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    S3ATestUtils.removeBaseAndBucketOverrides(conf,
        Constants.ALLOW_REQUESTER_PAYS,
        Constants.S3A_BUCKET_PROBE);
    return conf;
  }

  @Test
  public void testRequesterPaysOptionSuccess() throws Throwable {
    describe("Test requester pays enabled case by reading last then first byte");

    Configuration conf = this.createConfiguration();
    conf.setBoolean(Constants.ALLOW_REQUESTER_PAYS, true);
    // Enable bucket exists check, the first failure point people may encounter
    conf.setInt(Constants.S3A_BUCKET_PROBE, 2);

    Path requesterPaysPath = getRequesterPaysPath(conf);

    try (
        FileSystem fs = requesterPaysPath.getFileSystem(conf);
        FSDataInputStream inputStream = fs.open(requesterPaysPath);
    ) {
      long fileLength = fs.getFileStatus(requesterPaysPath).getLen();

      inputStream.seek(fileLength - 1);
      inputStream.readByte();

      // Jump back to the start, triggering a new GetObject request.
      inputStream.seek(0);
      inputStream.readByte();

      // Verify > 1 call was made, so we're sure it is correctly configured for each request
      IOStatisticAssertions
          .assertThatStatisticCounter(inputStream.getIOStatistics(),
              StreamStatisticNames.STREAM_READ_OPENED)
          .isGreaterThan(1);

      // Check list calls work without error
      fs.listFiles(requesterPaysPath.getParent(), false);
    }
  }

  @Test
  public void testRequesterPaysDisabledFails() throws Throwable {
    describe("Verify expected failure for requester pays buckets when client has it disabled");

    Configuration conf = this.createConfiguration();
    conf.setBoolean(Constants.ALLOW_REQUESTER_PAYS, false);
    Path requesterPaysPath = getRequesterPaysPath(conf);

    try (FileSystem fs = requesterPaysPath.getFileSystem(conf)) {
      intercept(
          AccessDeniedException.class,
          "403 Forbidden",
          "Expected requester pays bucket to fail without header set",
          () -> fs.open(requesterPaysPath).close()
      );
    }
  }

  private Path getRequesterPaysPath(Configuration conf) {
    String requesterPaysFile =
        conf.getTrimmed(KEY_REQUESTER_PAYS_FILE, DEFAULT_REQUESTER_PAYS_FILE);
    S3ATestUtils.assume(
        "Empty test property: " + KEY_REQUESTER_PAYS_FILE,
        !requesterPaysFile.isEmpty()
    );
    return new Path(requesterPaysFile);
  }

}
