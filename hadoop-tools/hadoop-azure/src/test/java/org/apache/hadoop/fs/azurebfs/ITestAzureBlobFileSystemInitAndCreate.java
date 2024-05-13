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

package org.apache.hadoop.fs.azurebfs;

import java.io.FileNotFoundException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.TrileanConversionException;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

/**
 * Test filesystem initialization and creation.
 */
public class ITestAzureBlobFileSystemInitAndCreate extends
    AbstractAbfsIntegrationTest {

  public ITestAzureBlobFileSystemInitAndCreate() throws Exception {
    this.getConfiguration().unset(ConfigurationKeys.AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION);
  }

  @Override
  public void setup() {
  }

  @Override
  public void teardown() {
  }

  @Test (expected = FileNotFoundException.class)
  public void ensureFilesystemWillNotBeCreatedIfCreationConfigIsNotSet() throws Exception {
    final AzureBlobFileSystem fs = this.createFileSystem();
    FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
  }

  @Test
  public void testGetAclCallOnHnsConfigAbsence() throws Exception {
    AzureBlobFileSystem fs = ((AzureBlobFileSystem) FileSystem.newInstance(
        getRawConfiguration()));
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    AbfsClient client = Mockito.spy(fs.getAbfsClient());
    Mockito.doReturn(client).when(store).getClient();

    Mockito.doThrow(TrileanConversionException.class)
        .when(store)
        .getNamespaceConfig();

    TracingContext tracingContext = getSampleTracingContext(fs, true);
    store.getIsNamespaceEnabled(tracingContext);

    Mockito.verify(client, Mockito.times(1))
        .getAclStatus(Mockito.anyString(), Mockito.any(TracingContext.class));
  }

  @Test
  public void testNoGetAclCallOnHnsConfigPresence() throws Exception {
    AzureBlobFileSystem fs = ((AzureBlobFileSystem) FileSystem.newInstance(
        getRawConfiguration()));
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    AbfsClient client = Mockito.spy(fs.getAbfsClient());
    Mockito.doReturn(client).when(store).getClient();

    Mockito.doReturn(true)
        .when(store)
        .getNamespaceConfig();

    TracingContext tracingContext = getSampleTracingContext(fs, true);
    store.getIsNamespaceEnabled(tracingContext);

    Mockito.verify(client, Mockito.times(0))
        .getAclStatus(Mockito.anyString(), Mockito.any(TracingContext.class));
  }
}
