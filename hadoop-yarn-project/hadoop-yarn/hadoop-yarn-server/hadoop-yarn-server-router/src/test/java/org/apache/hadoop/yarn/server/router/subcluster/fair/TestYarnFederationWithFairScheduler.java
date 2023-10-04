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
package org.apache.hadoop.yarn.server.router.subcluster.fair;

import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterInfo;
import org.apache.hadoop.yarn.server.router.subcluster.TestFederationSubCluster;
import org.apache.hadoop.yarn.server.router.webapp.dao.FederationClusterInfo;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.RM_WEB_SERVICE_PATH;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestYarnFederationWithFairScheduler {
  private static TestFederationSubCluster testFederationSubCluster;
  private static Set<String> subClusters;
  private static final String ROUTER_WEB_ADDRESS = "http://localhost:28089";

  @BeforeClass
  public static void setUp()
      throws IOException, InterruptedException, YarnException, TimeoutException {
    testFederationSubCluster = new TestFederationSubCluster();
    testFederationSubCluster.startFederationSubCluster(2182,
        "38032,38030,38031,38088,38033,SC-1,127.0.0.1:2182,fair-scheduler",
        "48032,48030,48031,48088,48033,SC-2,127.0.0.1:2182,fair-scheduler",
        "28050,28052,28089,127.0.0.1:2182");
    subClusters = Sets.newHashSet();
    subClusters.add("SC-1");
    subClusters.add("SC-2");
  }

  @AfterClass
  public static void shutDown() throws Exception {
    testFederationSubCluster.stop();
  }

  @Test
  public void testGetClusterInfo() throws InterruptedException, IOException {
    FederationClusterInfo federationClusterInfo =
        TestFederationSubCluster.performGetCalls(ROUTER_WEB_ADDRESS, RM_WEB_SERVICE_PATH,
        FederationClusterInfo.class, null, null);
    /*List<ClusterInfo> clusterInfos = federationClusterInfo.getList();
    assertNotNull(clusterInfos);
    assertEquals(2, clusterInfos.size());
    for (ClusterInfo clusterInfo : clusterInfos) {
      assertNotNull(clusterInfo);
      assertTrue(subClusters.contains(clusterInfo.getSubClusterId()));
    }*/
    Thread.sleep(2000000);
  }
}
