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
package org.apache.hadoop.yarn.server.router.ssproxy;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.dao.ApplicationHomeSubClusterDAO;
import org.apache.hadoop.yarn.server.federation.store.records.dao.SubClusterDeregisterDAO;
import org.apache.hadoop.yarn.server.federation.store.records.dao.SubClusterHeartbeatDAO;
import org.apache.hadoop.yarn.server.federation.store.records.dao.SubClusterInfoDAO;
import org.apache.hadoop.yarn.server.federation.store.records.dao.SubClusterPolicyConfigurationDAO;

import javax.ws.rs.core.Response;

/**
 * The protocol between clients and the FederationStateStore
 * over REST APIs
 */

public interface StateStoreWebServiceProtocol {

  /**
   * Register a <em>subcluster</em> by publishing capabilities as represented by
   * {@code SubClusterInfo} to indicate participation in federation. This is
   * typically done during initialization or restart/failover of the
   * subcluster's <code>ResourceManager</code>. Upon successful registration, an
   * identifier for the <em>subcluster</em> which is unique across the federated
   * cluster is returned. The identifier is static, i.e. preserved across
   * restarts and failover.
   *
   * @param scInfoDAO the capabilities of the subcluster that
   *          wants to participate in federation. The subcluster id is also
   *          specified in case registration is triggered by restart/failover
   * @return response empty on successfully if registration was successful
   * @throws YarnException if the request is invalid/fails
   */
  Response registerSubCluster(SubClusterInfoDAO scInfoDAO) throws YarnException;

  /**
   * Deregister a <em>subcluster</em> identified by {@code SubClusterId} to
   * change state in federation. This can be done to mark the sub cluster lost,
   * deregistered, or decommissioned.
   *
   * @param deregisterDAO - the request to deregister the
   *          sub-cluster from federation.
   * @return response empty on successfully deregistering the subcluster state
   * @throws YarnException if the request is invalid/fails
   */
  Response deregisterSubCluster(SubClusterDeregisterDAO deregisterDAO)
      throws YarnException;

  /**
   * Periodic heartbeat from a <code>ResourceManager</code> participating in
   * federation to indicate liveliness. The heartbeat publishes the current
   * capabilities as represented by {@code SubClusterInfo} of the subcluster.
   * Currently response is empty if the operation was successful, if not an
   * exception reporting reason for a failure.
   *
   * @param heartbeatDAO the capabilities of the subcluster that
   *          wants to keep alive its participation in federation
   * @return response currently empty on if heartbeat was successfully processed
   * @throws YarnException if the request is invalid/fails
   */
  Response subClusterHeartBeat(SubClusterHeartbeatDAO heartbeatDAO)
      throws YarnException;

  /**
   * Get the membership information of <em>subcluster</em> as identified by
   * {@code SubClusterId}. The membership information includes the cluster
   * endpoint and current capabilities as represented by {@code SubClusterInfo}.
   *
   * @param subClusterId the subcluster whose information is required
   * @return the {@code SubClusterInfo}, or {@code null} if there is no mapping
   *         for the subcluster
   * @throws YarnException if the request is invalid/fails
   */
  Response getSubCluster(String subClusterId) throws YarnException;

  /**
   * Get the membership information of all the <em>subclusters</em> that are
   * currently participating in federation. The membership information includes
   * the cluster endpoint and current capabilities as represented by
   * {@code SubClusterInfo}.
   *
   * @param filterInactiveSubclusters whether to filter inactive sub clusters
   * @return a map of {@code SubClusterInfo} keyed by the {@code SubClusterId}
   * @throws YarnException if the request is invalid/fails
   */
  Response getSubClusters(boolean filterInactiveSubclusters)
      throws YarnException;

  /**
   * Get a map of all queue-to-policy configurations.
   *
   * @return the policies for all currently active queues in the system
   * @throws YarnException if the request is invalid/fails
   */
  Response getPoliciesConfigurations() throws YarnException;

  /**
   * Get the policy configuration for a given queue.
   *
   * @param queue the queue whose {@code SubClusterPolicyConfiguration} is
   *          required
   * @return the {@code SubClusterPolicyConfiguration} for the specified queue,
   *         or {@code null} if there is no mapping for the queue
   * @throws YarnException if the request is invalid/fails
   */
  Response getPolicyConfiguration(String queue) throws YarnException;

  /**
   * Set the policy configuration for a given queue.
   *
   * @param policyConf the {@code SubClusterPolicyConfiguration} with the
   *          corresponding queue
   * @return response empty on successfully updating the
   *         {@code SubClusterPolicyConfiguration} for the specified queue
   * @throws YarnException if the request is invalid/fails
   */

  Response setPolicyConfiguration(SubClusterPolicyConfigurationDAO policyConf)
      throws YarnException;

  /**
   * Register the home {@code SubClusterId} of the newly submitted
   * {@code ApplicationId}. Currently response is empty if the operation was
   * successful, if not an exception reporting reason for a failure. If a
   * mapping for the application already existed, the {@code SubClusterId} in
   * this response will return the existing mapping which might be different
   * from that in the {@code AddApplicationHomeSubClusterRequest}.
   *
   * @param appHomeDAO the request to register a new application with its home
   *          sub-cluster
   * @return upon successful registration of the application in the StateStore,
   *         {@code AddApplicationHomeSubClusterRequest} containing the home
   *         sub-cluster of the application. Otherwise, an exception reporting
   *         reason for a failure
   * @throws YarnException if the request is invalid/fails
   */
  Response addApplicationHomeSubCluster(ApplicationHomeSubClusterDAO appHomeDAO)
      throws YarnException;

  /**
   * Update the home {@code SubClusterId} of a previously submitted
   * {@code ApplicationId}. Currently response is empty if the operation was
   * successful, if not an exception reporting reason for a failure.
   *
   * @param appHomeDAO the request to update the home sub-cluster of an
   *          application.
   * @return empty on successful update of the application in the StateStore, if
   *         not an exception reporting reason for a failure
   * @throws YarnException if the request is invalid/fails
   */
  Response updateApplicationHomeSubCluster(
      ApplicationHomeSubClusterDAO appHomeDAO) throws YarnException;

  /**
   * Get information about the application identified by the input
   * {@code ApplicationId}.
   *
   * @param appId the application to query
   * @return {@code ApplicationHomeSubCluster} containing the application's home
   *         subcluster
   * @throws YarnException if the request is invalid/fails
   */
  Response getApplicationHomeSubCluster(String appId) throws YarnException;

  /**
   * Get the {@code ApplicationHomeSubCluster} list representing the mapping of
   * all submitted applications to it's home sub-cluster.
   *
   * @return the mapping of all submitted application to it's home sub-cluster
   * @throws YarnException if the request is invalid/fails
   */
  Response getApplicationsHomeSubCluster() throws YarnException;

  /**
   * Delete the mapping of home {@code SubClusterId} of a previously submitted
   * {@code ApplicationId}. Currently response is empty if the operation was
   * successful, if not an exception reporting reason for a failure.
   *
   * @param appId the ID of the application to delete
   * @return empty on successful update of the application in the StateStore, if
   *         not an exception reporting reason for a failure
   * @throws YarnException if the request is invalid/fails
   */
  Response deleteApplicationHomeSubCluster(String appId) throws YarnException;
}
