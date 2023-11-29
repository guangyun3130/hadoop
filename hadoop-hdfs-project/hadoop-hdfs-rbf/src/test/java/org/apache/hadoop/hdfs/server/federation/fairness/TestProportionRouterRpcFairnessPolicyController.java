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
package org.apache.hadoop.hdfs.server.federation.fairness;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.federation.router.FederationUtil;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.util.Time;
import org.junit.Test;


import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.server.federation.fairness.RouterRpcFairnessConstants.CONCURRENT_NS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FAIRNESS_ACQUIRE_TIMEOUT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FAIR_HANDLER_PROPORTION_KEY_PREFIX;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test functionality of {@link ProportionRouterRpcFairnessPolicyController}.
 */
public class TestProportionRouterRpcFairnessPolicyController {
  private static String nameServices =
      "ns1.nn1, ns1.nn2, ns2.nn1, ns2.nn2";

  @Test
  public void testHandlerAllocationDefault() {
    RouterRpcFairnessPolicyController routerRpcFairnessPolicyController
        = getFairnessPolicyController(30);
    for (int i=0; i<3; i++) {
      assertTrue(routerRpcFairnessPolicyController.acquirePermit("ns1"));
      assertTrue(routerRpcFairnessPolicyController.acquirePermit("ns2"));
      assertTrue(
          routerRpcFairnessPolicyController.acquirePermit(CONCURRENT_NS));
    }
    assertFalse(routerRpcFairnessPolicyController.acquirePermit("ns1"));
    assertFalse(routerRpcFairnessPolicyController.acquirePermit("ns2"));
    assertFalse(routerRpcFairnessPolicyController.acquirePermit(CONCURRENT_NS));

    routerRpcFairnessPolicyController.releasePermit("ns1");
    routerRpcFairnessPolicyController.releasePermit("ns2");
    routerRpcFairnessPolicyController.releasePermit(CONCURRENT_NS);

    assertTrue(routerRpcFairnessPolicyController.acquirePermit("ns1"));
    assertTrue(routerRpcFairnessPolicyController.acquirePermit("ns2"));
    assertTrue(routerRpcFairnessPolicyController.acquirePermit(CONCURRENT_NS));
  }

  @Test
  public void testHandlerAllocationPreconfigured() {
    Configuration conf = createConf(40);
    conf.setDouble(DFS_ROUTER_FAIR_HANDLER_PROPORTION_KEY_PREFIX + "ns1", 0.5);
    RouterRpcFairnessPolicyController routerRpcFairnessPolicyController =
        FederationUtil.newFairnessPolicyController(conf);

    // ns1 should have 20 permits allocated
    for (int i=0; i<20; i++) {
      assertTrue(routerRpcFairnessPolicyController.acquirePermit("ns1"));
    }

    // ns2 should have 4 permits.
    // concurrent should have 4 permits.
    for (int i=0; i<4; i++) {
      assertTrue(routerRpcFairnessPolicyController.acquirePermit("ns2"));
      assertTrue(
          routerRpcFairnessPolicyController.acquirePermit(CONCURRENT_NS));
    }

    assertFalse(routerRpcFairnessPolicyController.acquirePermit("ns1"));
    assertFalse(routerRpcFairnessPolicyController.acquirePermit("ns2"));
    assertFalse(routerRpcFairnessPolicyController.acquirePermit(CONCURRENT_NS));
  }

  @Test
  public void testAcquireTimeout() {
    Configuration conf = createConf(40);
    conf.setDouble(DFS_ROUTER_FAIR_HANDLER_PROPORTION_KEY_PREFIX + "ns1", 0.5);
    conf.setTimeDuration(DFS_ROUTER_FAIRNESS_ACQUIRE_TIMEOUT, 100, TimeUnit.MILLISECONDS);
    RouterRpcFairnessPolicyController routerRpcFairnessPolicyController =
        FederationUtil.newFairnessPolicyController(conf);

    // ns1 should have 30 permits allocated
    for (int i = 0; i < 20; i++) {
      assertTrue(routerRpcFairnessPolicyController.acquirePermit("ns1"));
    }
    long acquireBeginTimeMs = Time.monotonicNow();
    assertFalse(routerRpcFairnessPolicyController.acquirePermit("ns1"));
    long acquireTimeMs = Time.monotonicNow() - acquireBeginTimeMs;

    // There are some other operations, so acquireTimeMs >= 100ms.
    assertTrue(acquireTimeMs >= 100);
  }

  @Test
  public void testAllocationWithZeroProportion() {
    Configuration conf = createConf(40);
    conf.setDouble(DFS_ROUTER_FAIR_HANDLER_PROPORTION_KEY_PREFIX + "ns1", 0);
    RouterRpcFairnessPolicyController routerRpcFairnessPolicyController =
        FederationUtil.newFairnessPolicyController(conf);

    // ns1 should have 1 permit allocated
    assertTrue(routerRpcFairnessPolicyController.acquirePermit("ns1"));
    assertFalse(routerRpcFairnessPolicyController.acquirePermit("ns1"));
  }

  @Test
  public void testAllocationHandlersGreaterThanCount() {
    Configuration conf = createConf(40);
    conf.setDouble(DFS_ROUTER_FAIR_HANDLER_PROPORTION_KEY_PREFIX + "ns1", 0.8);
    conf.setDouble(DFS_ROUTER_FAIR_HANDLER_PROPORTION_KEY_PREFIX + "ns2", 0.8);
    conf.setDouble(DFS_ROUTER_FAIR_HANDLER_PROPORTION_KEY_PREFIX + CONCURRENT_NS, 1);
    RouterRpcFairnessPolicyController routerRpcFairnessPolicyController =
        FederationUtil.newFairnessPolicyController(conf);

    // ns1 32 permit allocated
    // ns2 32 permit allocated
    for (int i = 0; i < 32; i++) {
      assertTrue(routerRpcFairnessPolicyController.acquirePermit("ns1"));
      assertTrue(routerRpcFairnessPolicyController.acquirePermit("ns2"));
    }
    // CONCURRENT_NS 40 permit allocated
    for (int i=0; i < 40; i++) {
      assertTrue(routerRpcFairnessPolicyController.acquirePermit(CONCURRENT_NS));
    }
  }

  private RouterRpcFairnessPolicyController getFairnessPolicyController(
      int handlers) {
    return FederationUtil.newFairnessPolicyController(createConf(handlers));
  }

  private Configuration createConf(int handlers) {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFS_ROUTER_HANDLER_COUNT_KEY, handlers);
    conf.set(DFS_ROUTER_MONITOR_NAMENODE, nameServices);
    conf.setClass(
        RBFConfigKeys.DFS_ROUTER_FAIRNESS_POLICY_CONTROLLER_CLASS,
        ProportionRouterRpcFairnessPolicyController.class,
        RouterRpcFairnessPolicyController.class);
    return conf;
  }
}
