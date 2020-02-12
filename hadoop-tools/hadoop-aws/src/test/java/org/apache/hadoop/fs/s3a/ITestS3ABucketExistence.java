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

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.LambdaTestUtils;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A;
import static org.apache.hadoop.fs.s3a.Constants.S3A_BUCKET_PROBE;
import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_METASTORE_NULL;
import static org.apache.hadoop.fs.s3a.Constants.S3_METADATA_STORE_IMPL;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Class to test bucket existence APIs.
 */
public class ITestS3ABucketExistence extends AbstractS3ATestBase {

  private FileSystem fs;

  private final String randomBucket =
          "random-bucket-" + UUID.randomUUID().toString();

  private final URI uri = URI.create(FS_S3A + "://" + randomBucket + "/");

  @Test
  public void testNoBucketProbing() throws Exception {
    describe("Disable init-time probes and expect FS operations to fail");
    Configuration conf = createConfigurationWithProbe(0);
    // metastores can bypass S3 checks, so disable S3Guard, always
    conf.set(S3_METADATA_STORE_IMPL, S3GUARD_METASTORE_NULL);

    fs = FileSystem.get(uri, conf);

    Path root = new Path(uri);

    expectUnknownStore(
        () -> fs.getFileStatus(root));

    expectUnknownStore(
        () -> fs.listStatus(root));

    Path src = new Path(root, "testfile");
    expectUnknownStore(
        () -> fs.getFileStatus(src));

    // the exception must not be caught and marked down to an FNFE
    expectUnknownStore(() -> fs.exists(src));
    expectUnknownStore(() -> fs.isFile(src));
    expectUnknownStore(() -> fs.isDirectory(src));
    expectUnknownStore(() -> fs.mkdirs(src));
    expectUnknownStore(() -> fs.delete(src));

    byte[] data = dataset(1024, 'a', 'z');
    expectUnknownStore(
        () -> writeDataset(fs, src, data, data.length, 1024 * 1024, true));
  }

  /**
   * Expect an operation to raise an UnknownStoreException.
   * @param eval closure
   * @param <T> return type of closure
   * @throws Exception anything else raised.
   */
  public static <T> void expectUnknownStore(
      Callable<T> eval)
      throws Exception {
    intercept(UnknownStoreException.class, eval);
  }

  /**
   * Expect an operation to raise an UnknownStoreException.
   * @param eval closure
   * @throws Exception anything else raised.
   */
  public static void expectUnknownStore(
      LambdaTestUtils.VoidCallable eval)
      throws Exception {
    intercept(UnknownStoreException.class, eval);
  }

  private Configuration createConfigurationWithProbe(final int probe) {
    Configuration conf = new Configuration(getFileSystem().getConf());
    S3ATestUtils.disableFilesystemCaching(conf);
    conf.setInt(S3A_BUCKET_PROBE, probe);
    return conf;
  }

  @Test
  public void testBucketProbingV1() throws Exception {
    Configuration configuration = createConfigurationWithProbe(1);
    expectUnknownStore(
        () -> FileSystem.get(uri, configuration));
  }

  @Test
  public void testBucketProbingV2() throws Exception {
    Configuration configuration = createConfigurationWithProbe(2);
    expectUnknownStore(
        () -> FileSystem.get(uri, configuration));
  }

  @Test
  public void testBucketProbingParameterValidation() throws Exception {
    Configuration configuration = createConfigurationWithProbe(3);
    intercept(IllegalArgumentException.class,
            "Value of " + S3A_BUCKET_PROBE + " should be between 0 to 2",
            "Should throw IllegalArgumentException",
        () -> FileSystem.get(uri, configuration));
    configuration.setInt(S3A_BUCKET_PROBE, -1);
    intercept(IllegalArgumentException.class,
            "Value of " + S3A_BUCKET_PROBE + " should be between 0 to 2",
            "Should throw IllegalArgumentException",
        () -> FileSystem.get(uri, configuration));
  }

  @Override
  protected Configuration getConfiguration() {
    Configuration configuration = super.getConfiguration();
    S3ATestUtils.disableFilesystemCaching(configuration);
    return configuration;
  }

  @Override
  public void teardown() throws Exception {
    IOUtils.cleanupWithLogger(getLogger(), fs);
    super.teardown();
  }
}
