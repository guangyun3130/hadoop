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

package org.apache.hadoop.fs.azurebfs.contracts.services;

import java.sql.Blob;

/**
 * Saves the different request parameters for append
 */
public class AppendRequestParameters {
  public enum Mode {
    APPEND_MODE,
    FLUSH_MODE,
    FLUSH_CLOSE_MODE
  }

  public class BlobEndpointParameters {
    private String blockId;
    private String eTag;

    public BlobEndpointParameters(String blockId, String eTag) {
      this.blockId = blockId;
      this.eTag = eTag;
    }

    public String getBlockId() {
      return blockId;
    }

    public String getETag() {
      return eTag;
    }

    public void setBlockId(String blockId) {
      this.blockId = blockId;
    }

    public void setETag(String eTag) {
      this.eTag = eTag;
    }
  }

  private final long position;
  private final int offset;
  private final int length;
  private final Mode mode;
  private final boolean isAppendBlob;
  private final String leaseId;
  private boolean isExpectHeaderEnabled;
  private boolean isRetryDueToExpect;

  /*
   * Following parameters are used by AbfsBlobClient only.
   * Blob Endpoint Append API requires blockId and eTag to be passed in the request.
   */
  private BlobEndpointParameters blobParams;

  // Constructor to be used for interacting with AbfsDfsClient
  public AppendRequestParameters(final long position,
      final int offset,
      final int length,
      final Mode mode,
      final boolean isAppendBlob,
      final String leaseId,
      final boolean isExpectHeaderEnabled) {
    this.position = position;
    this.offset = offset;
    this.length = length;
    this.mode = mode;
    this.isAppendBlob = isAppendBlob;
    this.leaseId = leaseId;
    this.isExpectHeaderEnabled = isExpectHeaderEnabled;
    this.isRetryDueToExpect = false;
    this.blobParams = new BlobEndpointParameters(null, null);
  }

  // Constructor to be used for interacting with AbfsBlobClient
  public AppendRequestParameters(final long position,
      final int offset,
      final int length,
      final Mode mode,
      final boolean isAppendBlob,
      final String leaseId,
      final boolean isExpectHeaderEnabled,
      final String blockId,
      final String eTag) {
    this.position = position;
    this.offset = offset;
    this.length = length;
    this.mode = mode;
    this.isAppendBlob = isAppendBlob;
    this.leaseId = leaseId;
    this.isExpectHeaderEnabled = isExpectHeaderEnabled;
    this.isRetryDueToExpect = false;
    this.blobParams = new BlobEndpointParameters(blockId, eTag);
  }

  public long getPosition() {
    return this.position;
  }

  public int getoffset() {
    return this.offset;
  }

  public int getLength() {
    return this.length;
  }

  public Mode getMode() {
    return this.mode;
  }

  public boolean isAppendBlob() {
    return this.isAppendBlob;
  }

  public String getLeaseId() {
    return this.leaseId;
  }

  public boolean isExpectHeaderEnabled() {
    return isExpectHeaderEnabled;
  }

  public boolean isRetryDueToExpect() {
    return isRetryDueToExpect;
  }

  /**
   * Returns BlockId of the block blob to be appended.
   * @return blockId
   */
  public String getBlockId() {
    return blobParams.getBlockId();
  }

  /**
   * Returns ETag of the block blob.
   * @return eTag
   */
  public String getETag() {
    return blobParams.getETag();
  }

  public void setRetryDueToExpect(boolean retryDueToExpect) {
    isRetryDueToExpect = retryDueToExpect;
  }

  public void setExpectHeaderEnabled(boolean expectHeaderEnabled) {
    isExpectHeaderEnabled = expectHeaderEnabled;
  }
}
