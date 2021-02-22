/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.rm;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;

/**
 * Tests to verify S3 Client-Side Encryption (CSE).
 */
public abstract class ITestS3AEncryptionCSE extends AbstractS3ATestBase {

  private static final List<Integer> SIZES =
      new ArrayList<>(Arrays.asList(0, 1, 255, 4095));

  /**
   * Testing S3 CSE on different file sizes.
   */
  @Test
  public void testEncryption() throws Throwable {
    describe("Test to verify client-side encryption for different file sizes.");
    for (int size : SIZES) {
      validateEncryptionForFilesize(size);
    }
  }

  /**
   * Testing the S3 client side encryption over rename operation.
   */
  @Test
  public void testEncryptionOverRename() throws Throwable {
    describe("Test for AWS CSE on Rename Operation.");
    skipTest();
    S3AFileSystem fs = getFileSystem();
    Path src = path(getMethodName());
    byte[] data = dataset(1024, 'a', 'z');
    LOG.info("Region used: {}", fs.getAmazonS3Client().getRegionName());
    writeDataset(fs, src, data, data.length, 1024 * 1024,
        true, false);

    //ContractTestUtils.verifyFileContents(fs, src, data);
    Path dest = path(src.getName() + "-copy");
    fs.rename(src, dest);
    ContractTestUtils.verifyFileContents(fs, dest, data);
    assertEncrypted(dest);
  }

  /**
   * Test to verify if we get same content length of files in S3 CSE using
   * listStatus on the parent directory.
   */
  @Test
  public void testDirectoryListingFileLengths() throws IOException {
    describe("Test to verify directory listing calls gives correct content "
        + "lengths");
    S3AFileSystem fs = getFileSystem();
    Path parentDir = path(getMethodName());

    // Creating files in the parent directory that will be used to assert
    // content length.
    for (int i : SIZES) {
      Path child = new Path(parentDir, getMethodName() + i);
      writeThenReadFile(child, i);
    }

    // Getting the content lengths of files inside the directory via
    // directory listing.
    List<Integer> fileLengthDirListing = new ArrayList<>();
    for (FileStatus fileStatus : fs.listStatus(parentDir)) {
      fileLengthDirListing.add((int) fileStatus.getLen());
    }

    // Assert the file length we got against expected file length.
    Assertions.assertThat(fileLengthDirListing)
        .describedAs("File length isn't same "
            + "as expected from directory listing").containsExactlyInAnyOrderElementsOf(SIZES);

  }

  /**
   * Method to validate CSE for different file sizes.
   *
   * @param len length of the file.
   */
  protected void validateEncryptionForFilesize(int len) throws IOException {
    skipTest();
    describe("Create an encrypted file of size " + len);
    // Creating a unique path by adding file length in file name.
    Path path = writeThenReadFile(getMethodName() + len, len);
    assertEncrypted(path);
    rm(getFileSystem(), path, false, false);
  }

  /**
   * Skip tests if certain conditions are met.
   */
  protected abstract void skipTest();

  /**
   * Assert that at path references an encrypted blob.
   *
   * @param path path
   * @throws IOException on a failure
   */
  protected abstract void assertEncrypted(Path path) throws IOException;

}
