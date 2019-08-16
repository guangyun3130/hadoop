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

package org.apache.hadoop.ozone.om.response.security;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.utils.db.BatchOperation;
import org.apache.hadoop.utils.db.Table;

import java.io.IOException;

/**
 * Handle response for DelegationToken request.
 */
public class OMDelegationTokenResponse extends OMClientResponse {

  private OzoneTokenIdentifier ozoneTokenIdentifier;
  private long renewTime;
  public OMDelegationTokenResponse(OzoneTokenIdentifier ozoneTokenIdentifier,
      long renewTime, OMResponse omResponse) {
    super(omResponse);
    this.ozoneTokenIdentifier = ozoneTokenIdentifier;
    this.renewTime = renewTime;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    Table table = omMetadataManager.getDelegationTokenTable();
    if (getOMResponse().getStatus() == OzoneManagerProtocolProtos.Status.OK) {
      if (OzoneManagerProtocolProtos.Type.GetDelegationToken ==
          getOMResponse().getCmdType()) {
        table.putWithBatch(batchOperation,
            ozoneTokenIdentifier, renewTime);
      } else if (OzoneManagerProtocolProtos.Type.CancelDelegationToken ==
          getOMResponse().getCmdType()) {
        table.deleteWithBatch(batchOperation, ozoneTokenIdentifier);
      }
    }
  }
}

