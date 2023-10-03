/**
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

package org.apache.hadoop.yarn.logaggregation.filecontroller;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;
import org.junit.Test;


import static org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileController.TLDIR_PERMISSIONS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;

/**
 * Test for the abstract {@link LogAggregationFileController} class,
 * checking its core functionality.
 */
public class TestLogAggregationFileController {

  @Test
  public void testRemoteDirCreationWithCustomUser() throws Exception {
    LogAggregationFileController controller = mock(
        LogAggregationFileController.class, Mockito.CALLS_REAL_METHODS);
    FileSystem fs = mock(FileSystem.class);
    setupCustomUserMocks(controller, fs, "/tmp/logs");

    controller.initialize(new Configuration(), "TFile");
    controller.fsSupportsChmod = false;

    controller.verifyAndCreateRemoteLogDir();
    assertPermissionFileWasUsedOneTime(fs);
    Assert.assertTrue(controller.fsSupportsChmod);

    doThrow(new UnsupportedOperationException()).when(fs).setPermission(any(), any());
    controller.verifyAndCreateRemoteLogDir();
    assertPermissionFileWasUsedOneTime(fs); // still once -> cached
    Assert.assertTrue(controller.fsSupportsChmod);

    controller.fsSupportsChmod = false;
    controller.verifyAndCreateRemoteLogDir();
    assertPermissionFileWasUsedOneTime(fs); // still once -> cached
    Assert.assertTrue(controller.fsSupportsChmod);
  }

  @Test
  public void testRemoteDirCreationWithCustomUserFsChmodNotSupported() throws Exception {
    LogAggregationFileController controller = mock(
        LogAggregationFileController.class, Mockito.CALLS_REAL_METHODS);
    FileSystem fs = mock(FileSystem.class);
    setupCustomUserMocks(controller, fs, "/tmp/logs2");
    doThrow(new UnsupportedOperationException()).when(fs).setPermission(any(), any());

    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR, "/tmp/logs2");
    controller.initialize(conf, "TFile");
    controller.verifyAndCreateRemoteLogDir();
    assertPermissionFileWasUsedOneTime(fs);
    Assert.assertFalse(controller.fsSupportsChmod);

    controller.verifyAndCreateRemoteLogDir();
    assertPermissionFileWasUsedOneTime(fs); // still once -> cached
    Assert.assertFalse(controller.fsSupportsChmod);

    controller.fsSupportsChmod = true;
    controller.verifyAndCreateRemoteLogDir();
    assertPermissionFileWasUsedOneTime(fs); // still once -> cached
    Assert.assertFalse(controller.fsSupportsChmod);
  }

  private static void setupCustomUserMocks(LogAggregationFileController controller,
                                           FileSystem fs, String path)
      throws URISyntaxException, IOException {
    doReturn(new URI("")).when(fs).getUri();
    doReturn(new FileStatus(128, false, 0, 64, System.currentTimeMillis(),
        System.currentTimeMillis(), new FsPermission(TLDIR_PERMISSIONS),
        "not_yarn_user", "yarn_group", new Path(path))).when(fs)
        .getFileStatus(any(Path.class));
    doReturn(fs).when(controller).getFileSystem(any(Configuration.class));

    UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
        "yarn_user", new String[]{"yarn_group", "other_group"});
    UserGroupInformation.setLoginUser(ugi);
  }

  private static void assertPermissionFileWasUsedOneTime(FileSystem fs) throws IOException {
    verify(fs, times(1)).createNewFile(any());
    verify(fs, times(1)).setPermission(any(), eq(new FsPermission(TLDIR_PERMISSIONS)));
    verify(fs, times(1)).delete(any(), eq(false));
  }
}
