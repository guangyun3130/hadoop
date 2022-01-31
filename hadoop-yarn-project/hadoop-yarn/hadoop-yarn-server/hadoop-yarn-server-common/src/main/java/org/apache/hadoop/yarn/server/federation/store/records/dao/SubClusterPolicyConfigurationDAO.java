/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.yarn.server.federation.store.records.dao;

import org.apache.commons.net.util.Base64;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.nio.ByteBuffer;

/**
 * {@link SubClusterPolicyConfigurationDAO} is a class that represents a
 * configuration of a policy. For a single queue, it contains a policy type
 * (resolve to a class name) and its params as an opaque {@link ByteBuffer}.
 *
 * Note: by design the params are an opaque ByteBuffer, this allows for enough
 * flexibility to evolve the policies without impacting the protocols to/from
 * the federation state store.
 */
@XmlRootElement(name = "subClusterPolicyConfigurationDAO")
@XmlAccessorType(XmlAccessType.FIELD)
public class SubClusterPolicyConfigurationDAO {
  public String queueName;
  public String policyType;
  public String policyParams;

  public SubClusterPolicyConfigurationDAO() {
  } // JAXB needs this

  public SubClusterPolicyConfigurationDAO(
      SubClusterPolicyConfiguration policy) {
    this.queueName = policy.getQueue();
    this.policyType = policy.getType();
    this.policyParams = Base64.encodeBase64String(policy.getParams().array());
  }

  public SubClusterPolicyConfiguration toSubClusterPolicyConfiguration() {
    return SubClusterPolicyConfiguration.newInstance(queueName, policyType,
        ByteBuffer.wrap(Base64.decodeBase64(policyParams)));
  }
}
