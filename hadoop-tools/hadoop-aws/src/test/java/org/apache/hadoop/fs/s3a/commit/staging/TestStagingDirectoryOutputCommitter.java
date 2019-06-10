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

package org.apache.hadoop.fs.s3a.commit.staging;

import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.fs.s3a.commit.InternalCommitterConstants;

import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;
import static org.apache.hadoop.fs.s3a.commit.staging.StagingTestBase.*;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.mockito.Mockito.*;

/** Mocking test of directory committer. */
public class TestStagingDirectoryOutputCommitter
    extends StagingTestBase.JobCommitterTest<DirectoryStagingCommitter> {

  @Override
  DirectoryStagingCommitter newJobCommitter() throws Exception {
    return new DirectoryStagingCommitter(outputPath,
        createTaskAttemptForJob());
  }

  @Test
  public void testBadConflictMode() throws Throwable {
    getJob().getConfiguration().set(
        FS_S3A_COMMITTER_STAGING_CONFLICT_MODE, "merge");
    intercept(IllegalArgumentException.class,
        "MERGE", "committer conflict",
        this::newJobCommitter);
  }

  @Test
  public void testDefaultConflictResolution() throws Exception {
    getJob().getConfiguration().unset(
        FS_S3A_COMMITTER_STAGING_CONFLICT_MODE);
    pathIsDirectory(getMockS3A(), outputPath);
    verifyJobSetupAndCommit();
  }

  @Test
  public void testFailConflictResolution() throws Exception {
    getJob().getConfiguration().set(
        FS_S3A_COMMITTER_STAGING_CONFLICT_MODE, CONFLICT_MODE_FAIL);
    verifyFailureConflictOutcome();
  }

  protected void verifyFailureConflictOutcome() throws Exception {
    pathIsDirectory(getMockS3A(), outputPath);
    final DirectoryStagingCommitter committer = newJobCommitter();

    // this should fail
    intercept(PathExistsException.class,
        InternalCommitterConstants.E_DEST_EXISTS,
        "Should throw an exception because the path exists",
        () -> committer.setupJob(getJob()));

    // but there are no checks in job commit (HADOOP-15469)
    committer.commitJob(getJob());

    reset((FileSystem) getMockS3A());
    pathDoesNotExist(getMockS3A(), outputPath);

    committer.setupJob(getJob());
    verifyExistenceChecked(getMockS3A(), outputPath);
    verifyMkdirsInvoked(getMockS3A(), outputPath);
    verifyNoMoreInteractions((FileSystem) getMockS3A());

    reset((FileSystem) getMockS3A());
    pathDoesNotExist(getMockS3A(), outputPath);
    committer.commitJob(getJob());
    verifyCompletion(getMockS3A());
  }

  @Test
  public void testAppendConflictResolution() throws Exception {

    getJob().getConfiguration().set(
        FS_S3A_COMMITTER_STAGING_CONFLICT_MODE, CONFLICT_MODE_APPEND);
    FileSystem mockS3 = getMockS3A();
    pathIsDirectory(mockS3, outputPath);
    verifyJobSetupAndCommit();
  }

  protected void verifyJobSetupAndCommit()
      throws Exception {
    final DirectoryStagingCommitter committer = newJobCommitter();

    committer.setupJob(getJob());
    FileSystem mockS3 = getMockS3A();

    Mockito.reset(mockS3);
    pathExists(mockS3, outputPath);

    committer.commitJob(getJob());
    verifyCompletion(mockS3);
  }

  @Test
  public void testReplaceConflictResolution() throws Exception {
    FileSystem mockS3 = getMockS3A();

    pathIsDirectory(mockS3, outputPath);

    getJob().getConfiguration().set(
        FS_S3A_COMMITTER_STAGING_CONFLICT_MODE, CONFLICT_MODE_REPLACE);

    final DirectoryStagingCommitter committer = newJobCommitter();

    committer.setupJob(getJob());

    Mockito.reset(mockS3);
    pathExists(mockS3, outputPath);
    canDelete(mockS3, outputPath);

    committer.commitJob(getJob());
    verifyDeleted(mockS3, outputPath);
    verifyCompletion(mockS3);
  }

  @Test
  public void testReplaceConflictFailsIfDestIsFile() throws Exception {
    pathIsFile(getMockS3A(), outputPath);

    getJob().getConfiguration().set(
        FS_S3A_COMMITTER_STAGING_CONFLICT_MODE, CONFLICT_MODE_REPLACE);

    intercept(PathExistsException.class,
        InternalCommitterConstants.E_DEST_EXISTS,
        "Expected a PathExistsException as a the destination"
            + " is a file",
        () -> {
          newJobCommitter().setupJob(getJob());
        });
  }

  @Test
  public void testAppendConflictFailsIfDestIsFile() throws Exception {
    pathIsFile(getMockS3A(), outputPath);

    getJob().getConfiguration().set(
        FS_S3A_COMMITTER_STAGING_CONFLICT_MODE, CONFLICT_MODE_APPEND);

    intercept(PathExistsException.class,
        InternalCommitterConstants.E_DEST_EXISTS,
        "Expected a PathExistsException as a the destination"
            + " is a file",
        () -> {
          newJobCommitter().setupJob(getJob());
        });
  }

}
