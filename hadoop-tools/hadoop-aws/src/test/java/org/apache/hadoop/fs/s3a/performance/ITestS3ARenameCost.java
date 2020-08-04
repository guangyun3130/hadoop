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

package org.apache.hadoop.fs.s3a.performance;


import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import static org.apache.hadoop.fs.s3a.Statistic.*;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.*;

/**
 * Use metrics to assert about the cost of file API calls.
 * Parameterized on guarded vs raw. and directory marker keep vs delete
 */
@RunWith(Parameterized.class)
public class ITestS3ARenameCost extends AbstractS3ACostTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3ARenameCost.class);

  /**
   * Parameterization.
   */
  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {"raw-keep-markers", false, true, false},
        {"raw-delete-markers", false, false, false},
        {"nonauth-keep-markers", true, true, false},
        {"auth-delete-markers", true, false, true}
    });
  }

  public ITestS3ARenameCost(final String name,
      final boolean s3guard,
      final boolean keepMarkers,
      final boolean authoritative) {
    super(s3guard, keepMarkers, authoritative);
  }

  @Test
  public void testRenameFileToDifferentDirectory() throws Throwable {
    describe("rename a file to a different directory, "
        + "keeping the source dir present");

    Path baseDir = dir(methodPath());

    Path srcDir = new Path(baseDir, "1/2/3/4/5/6");
    final Path srcFilePath = file(new Path(srcDir, "source.txt"));

    // create a new source file.
    // Explicitly use a new path object to guarantee that the parent paths
    // are different object instances and so equals() rather than ==
    // is
    Path parent2 = srcFilePath.getParent();
    Path srcFile2 = file(new Path(parent2, "source2.txt"));
    Assertions.assertThat(srcDir)
        .isNotSameAs(parent2);
    Assertions.assertThat(srcFilePath.getParent())
        .isEqualTo(srcFile2.getParent());

    // create a directory tree, expect the dir to be created and
    // possibly a request to delete all parent directories made.
    Path destBaseDir = new Path(baseDir, "dest");
    Path destDir = dir(new Path(destBaseDir, "a/b/c/d"));
    Path destFilePath = new Path(destDir, "dest.txt");

    // rename the source file to the destination file.
    // this tests file rename, not dir rename
    // as srcFile2 exists, the parent dir of srcFilePath must not be created.
    verifyMetrics(() ->
            execRename(srcFilePath, destFilePath),
        whenRaw(RENAME_SINGLE_FILE_DIFFERENT_DIR),
        always(DIRECTORIES_CREATED, 0),
        always(DIRECTORIES_DELETED, 0),
        // keeping: only the core delete operation is issued.
        whenKeeping(OBJECT_DELETE_REQUESTS, DELETE_OBJECT_REQUEST),
        whenKeeping(FAKE_DIRECTORIES_DELETED, 0),
        // deleting: delete any fake marker above the destination.
        whenDeleting(OBJECT_DELETE_REQUESTS,
            DELETE_OBJECT_REQUEST + DELETE_MARKER_REQUEST),
        whenDeleting(FAKE_DIRECTORIES_DELETED, directoriesInPath(destDir)));

    assertIsFile(destFilePath);
    assertIsDirectory(srcDir);
    assertPathDoesNotExist("should have gone in the rename", srcFilePath);
  }

  /**
   * Same directory rename is lower cost as there's no need to
   * look for the parent dir of the dest path or worry about
   * deleting markers.
   */
  @Test
  public void testRenameSameDirectory() throws Throwable {
    describe("rename a file to the same directory");

    Path baseDir = dir(methodPath());
    final Path sourceFile = file(new Path(baseDir, "source.txt"));

    // create a new source file.
    // Explicitly use a new path object to guarantee that the parent paths
    // are different object instances and so equals() rather than ==
    // is
    Path parent2 = sourceFile.getParent();
    Path destFile = new Path(parent2, "dest");
    verifyMetrics(() ->
            execRename(sourceFile, destFile),
        whenRaw(RENAME_SINGLE_FILE_SAME_DIR),
        always(OBJECT_COPY_REQUESTS, 1),
        always(DIRECTORIES_CREATED, 0),
        always(OBJECT_DELETE_REQUESTS, DELETE_OBJECT_REQUEST),
        always(FAKE_DIRECTORIES_DELETED, 0));
  }

  @Test
  public void testCostOfRootFileRename() throws Throwable {
    describe("assert that a root file rename doesn't"
        + " do much in terms of parent dir operations");
    S3AFileSystem fs = getFileSystem();

    // unique name, so that even when run in parallel tests, there's no conflict
    String uuid = UUID.randomUUID().toString();
    Path src = file(new Path("/src-" + uuid));
    Path dest = new Path("/dest-" + uuid);
    try {
      verifyMetrics(() -> {
        fs.rename(src, dest);
        return "after fs.rename(/src,/dest) " + getMetricSummary();
      },
          whenRaw(FILE_STATUS_FILE_PROBE
              .plus(GET_FILE_STATUS_FNFE)
              .plus(COPY_OP)),
          // here we expect there to be no fake directories
          always(DIRECTORIES_CREATED, 0),
          // one for the renamed file only
          always(OBJECT_DELETE_REQUESTS,
              DELETE_OBJECT_REQUEST),
          // no directories are deleted: This is root
          always(DIRECTORIES_DELETED, 0),
          // no fake directories are deleted: This is root
          always(FAKE_DIRECTORIES_DELETED, 0),
          always(FILES_DELETED, 1));
    } finally {
      fs.delete(src, false);
      fs.delete(dest, false);
    }
  }

  @Test
  public void testCostOfRootFileDelete() throws Throwable {
    describe("assert that a root file delete doesn't"
        + " do much in terms of parent dir operations");
    S3AFileSystem fs = getFileSystem();

    // unique name, so that even when run in parallel tests, there's no conflict
    String uuid = UUID.randomUUID().toString();
    Path src = file(new Path("/src-" + uuid));
    try {
      // delete that destination file, assert only the file delete was issued
      verifyMetrics(() -> {
        fs.delete(src, false);
        return "after fs.delete(/dest) " + getMetricSummary();
      },
          always(DIRECTORIES_CREATED, 0),
          always(DIRECTORIES_DELETED, 0),
          always(FAKE_DIRECTORIES_DELETED, 0),
          always(FILES_DELETED, 1),
          always(OBJECT_DELETE_REQUESTS, DELETE_OBJECT_REQUEST),
          whenRaw(FILE_STATUS_FILE_PROBE)); /* no need to look at parent. */

    } finally {
      fs.delete(src, false);
    }
  }

}
