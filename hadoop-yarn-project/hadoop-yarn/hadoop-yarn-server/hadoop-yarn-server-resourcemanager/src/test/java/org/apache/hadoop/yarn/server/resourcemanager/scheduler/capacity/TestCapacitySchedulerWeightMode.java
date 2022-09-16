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


package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

public class TestCapacitySchedulerWeightMode {
  private static final String A = CapacitySchedulerConfiguration.ROOT + ".a";
  private static final String B = CapacitySchedulerConfiguration.ROOT + ".b";
  private static final String A1 = A + ".a1";
  private static final String B1 = B + ".b1";
  private static final String B2 = B + ".b2";

  private YarnConfiguration conf;

  RMNodeLabelsManager mgr;

  @BeforeEach
  public void setUp() throws Exception {
    conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
  }

  public static <E> Set<E> toSet(E... elements) {
    Set<E> set = Sets.newHashSet(elements);
    return set;
  }

  public static CapacitySchedulerConfiguration getConfigWithInheritedAccessibleNodeLabel(
      Configuration config) {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration(
        config);

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] { "a"});

    conf.setCapacityByLabel(A, RMNodeLabelsManager.NO_LABEL, 100f);
    conf.setCapacityByLabel(A, "newLabel", 100f);
    conf.setAccessibleNodeLabels(A, toSet("newLabel"));
    conf.setAllowZeroCapacitySum(A, true);

    // Define 2nd-level queues
    conf.setQueues(A, new String[] { "a1" });
    conf.setCapacityByLabel(A1, RMNodeLabelsManager.NO_LABEL, 100f);

    return conf;
  }


  /*
   * Queue structure:
   *                      root (*)
   *                  ________________
   *                 /                \
   *               a x(weight=100), y(w=50)   b y(w=50), z(w=100)
   *               ________________    ______________
   *              /                   /              \
   *             a1 ([x,y]: w=100)    b1(no)          b2([y,z]: w=100)
   */
  public static Configuration getCSConfWithQueueLabelsWeightOnly(
      Configuration config) {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration(
        config);

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] { "a", "b" });
    conf.setLabeledQueueWeight(CapacitySchedulerConfiguration.ROOT, "x", 100);
    conf.setLabeledQueueWeight(CapacitySchedulerConfiguration.ROOT, "y", 100);
    conf.setLabeledQueueWeight(CapacitySchedulerConfiguration.ROOT, "z", 100);

    conf.setLabeledQueueWeight(A, RMNodeLabelsManager.NO_LABEL, 1);
    conf.setMaximumCapacity(A, 10);
    conf.setAccessibleNodeLabels(A, toSet("x", "y"));
    conf.setLabeledQueueWeight(A, "x", 100);
    conf.setLabeledQueueWeight(A, "y", 50);

    conf.setLabeledQueueWeight(B, RMNodeLabelsManager.NO_LABEL, 9);
    conf.setMaximumCapacity(B, 100);
    conf.setAccessibleNodeLabels(B, toSet("y", "z"));
    conf.setLabeledQueueWeight(B, "y", 50);
    conf.setLabeledQueueWeight(B, "z", 100);

    // Define 2nd-level queues
    conf.setQueues(A, new String[] { "a1" });
    conf.setLabeledQueueWeight(A1, RMNodeLabelsManager.NO_LABEL, 100);
    conf.setMaximumCapacity(A1, 100);
    conf.setAccessibleNodeLabels(A1, toSet("x", "y"));
    conf.setDefaultNodeLabelExpression(A1, "x");
    conf.setLabeledQueueWeight(A1, "x", 100);
    conf.setLabeledQueueWeight(A1, "y", 100);

    conf.setQueues(B, new String[] { "b1", "b2" });
    conf.setLabeledQueueWeight(B1, RMNodeLabelsManager.NO_LABEL, 50);
    conf.setMaximumCapacity(B1, 50);
    conf.setAccessibleNodeLabels(B1, RMNodeLabelsManager.EMPTY_STRING_SET);

    conf.setLabeledQueueWeight(B2, RMNodeLabelsManager.NO_LABEL, 50);
    conf.setMaximumCapacity(B2, 50);
    conf.setAccessibleNodeLabels(B2, toSet("y", "z"));
    conf.setLabeledQueueWeight(B2, "y", 100);
    conf.setLabeledQueueWeight(B2, "z", 100);

    return conf;
  }

  /*
   * Queue structure:
   *                      root (*)
   *                  _______________________
   *                 /                       \
   *               a x(weight=100), y(w=50)   b y(w=50), z(w=100)
   *               ________________             ______________
   *              /                           /              \
   *             a1 ([x,y]: pct=100%)    b1(no)          b2([y,z]: percent=100%)
   *
   * Parent uses weight, child uses percentage
   */
  public static Configuration getCSConfWithLabelsParentUseWeightChildUsePct(
      Configuration config) {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration(
        config);

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] { "a", "b" });
    conf.setLabeledQueueWeight(CapacitySchedulerConfiguration.ROOT, "x", 100);
    conf.setLabeledQueueWeight(CapacitySchedulerConfiguration.ROOT, "y", 100);
    conf.setLabeledQueueWeight(CapacitySchedulerConfiguration.ROOT, "z", 100);

    conf.setLabeledQueueWeight(A, RMNodeLabelsManager.NO_LABEL, 1);
    conf.setMaximumCapacity(A, 10);
    conf.setAccessibleNodeLabels(A, toSet("x", "y"));
    conf.setLabeledQueueWeight(A, "x", 100);
    conf.setLabeledQueueWeight(A, "y", 50);

    conf.setLabeledQueueWeight(B, RMNodeLabelsManager.NO_LABEL, 9);
    conf.setMaximumCapacity(B, 100);
    conf.setAccessibleNodeLabels(B, toSet("y", "z"));
    conf.setLabeledQueueWeight(B, "y", 50);
    conf.setLabeledQueueWeight(B, "z", 100);

    // Define 2nd-level queues
    conf.setQueues(A, new String[] { "a1" });
    conf.setCapacityByLabel(A1, RMNodeLabelsManager.NO_LABEL, 100);
    conf.setMaximumCapacity(A1, 100);
    conf.setAccessibleNodeLabels(A1, toSet("x", "y"));
    conf.setDefaultNodeLabelExpression(A1, "x");
    conf.setCapacityByLabel(A1, "x", 100);
    conf.setCapacityByLabel(A1, "y", 100);

    conf.setQueues(B, new String[] { "b1", "b2" });
    conf.setCapacityByLabel(B1, RMNodeLabelsManager.NO_LABEL, 50);
    conf.setMaximumCapacity(B1, 50);
    conf.setAccessibleNodeLabels(B1, RMNodeLabelsManager.EMPTY_STRING_SET);

    conf.setCapacityByLabel(B2, RMNodeLabelsManager.NO_LABEL, 50);
    conf.setMaximumCapacity(B2, 50);
    conf.setAccessibleNodeLabels(B2, toSet("y", "z"));
    conf.setCapacityByLabel(B2, "y", 100);
    conf.setCapacityByLabel(B2, "z", 100);

    return conf;
  }

  /*
   * Queue structure:
   *                      root (*)
   *                  _______________________
   *                 /                       \
   *               a x(=100%), y(50%)   b y(=50%), z(=100%)
   *               ________________             ______________
   *              /                           /              \
   *             a1 ([x,y]: w=1)    b1(no)          b2([y,z]: w=1)
   *
   * Parent uses percentages, child uses weights
   */
  public static Configuration getCSConfWithLabelsParentUsePctChildUseWeight(
      Configuration config) {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration(
        config);

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] { "a", "b" });
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "x", 100);
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "y", 100);
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "z", 100);

    conf.setCapacityByLabel(A, RMNodeLabelsManager.NO_LABEL, 10);
    conf.setMaximumCapacity(A, 10);
    conf.setAccessibleNodeLabels(A, toSet("x", "y"));
    conf.setCapacityByLabel(A, "x", 100);
    conf.setCapacityByLabel(A, "y", 50);

    conf.setCapacityByLabel(B, RMNodeLabelsManager.NO_LABEL, 90);
    conf.setMaximumCapacity(B, 100);
    conf.setAccessibleNodeLabels(B, toSet("y", "z"));
    conf.setCapacityByLabel(B, "y", 50);
    conf.setCapacityByLabel(B, "z", 100);

    // Define 2nd-level queues
    conf.setQueues(A, new String[] { "a1" });
    conf.setLabeledQueueWeight(A1, RMNodeLabelsManager.NO_LABEL, 1);
    conf.setMaximumCapacity(A1, 100);
    conf.setAccessibleNodeLabels(A1, toSet("x", "y"));
    conf.setDefaultNodeLabelExpression(A1, "x");
    conf.setLabeledQueueWeight(A1, "x", 1);
    conf.setLabeledQueueWeight(A1, "y", 1);

    conf.setQueues(B, new String[] { "b1", "b2" });
    conf.setLabeledQueueWeight(B1, RMNodeLabelsManager.NO_LABEL, 1);
    conf.setMaximumCapacity(B1, 50);
    conf.setAccessibleNodeLabels(B1, RMNodeLabelsManager.EMPTY_STRING_SET);

    conf.setLabeledQueueWeight(B2, RMNodeLabelsManager.NO_LABEL, 1);
    conf.setMaximumCapacity(B2, 50);
    conf.setAccessibleNodeLabels(B2, toSet("y", "z"));
    conf.setLabeledQueueWeight(B2, "y", 1);
    conf.setLabeledQueueWeight(B2, "z", 1);

    return conf;
  }

  /**
   * This is an identical test of
   * @see {@link TestNodeLabelContainerAllocation#testContainerAllocateWithComplexLabels()}
   * The only difference is, instead of using label, it uses weight mode
   * @throws Exception
   */
  @Timeout(300000)  @Test
  void testContainerAllocateWithComplexLabelsWeightOnly() throws Exception {
    internalTestContainerAllocationWithNodeLabel(
        getCSConfWithQueueLabelsWeightOnly(conf));
  }

  /**
   * This is an identical test of
   * @see {@link TestNodeLabelContainerAllocation#testContainerAllocateWithComplexLabels()}
   * The only difference is, instead of using label, it uses weight mode:
   * Parent uses weight, child uses percent
   * @throws Exception
   */
  @Timeout(300000)  @Test
  void testContainerAllocateWithComplexLabelsWeightAndPercentMixed1() throws Exception {
    internalTestContainerAllocationWithNodeLabel(
        getCSConfWithLabelsParentUseWeightChildUsePct(conf));
  }

  /**
   * This is an identical test of
   * @see {@link TestNodeLabelContainerAllocation#testContainerAllocateWithComplexLabels()}
   * The only difference is, instead of using label, it uses weight mode:
   * Parent uses percent, child uses weight
   * @throws Exception
   */
  @Timeout(300000)  @Test
  void testContainerAllocateWithComplexLabelsWeightAndPercentMixed2() throws Exception {
    internalTestContainerAllocationWithNodeLabel(
        getCSConfWithLabelsParentUsePctChildUseWeight(conf));
  }

  /**
   * This checks whether the parent prints the correct log about the
   * configured mode.
   */
  @Timeout(300000)  @Test
  void testGetCapacityOrWeightStringUsingWeights() throws IOException {
    try (MockRM rm = new MockRM(
        getCSConfWithQueueLabelsWeightOnly(conf))) {
      rm.start();
      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

      String capacityOrWeightString = ((ParentQueue) cs.getQueue(A))
          .getCapacityOrWeightString();
      validateCapacityOrWeightString(capacityOrWeightString, true);

      capacityOrWeightString = ((LeafQueue) cs.getQueue(A1))
          .getCapacityOrWeightString();
      validateCapacityOrWeightString(capacityOrWeightString, true);

      capacityOrWeightString = ((LeafQueue) cs.getQueue(A1))
          .getExtendedCapacityOrWeightString();
      validateCapacityOrWeightString(capacityOrWeightString, true);
    }
  }

  /**
   * This checks whether the parent prints the correct log about the
   * configured mode.
   */
  @Timeout(300000)  @Test
  void testGetCapacityOrWeightStringParentPctLeafWeights()
      throws IOException {
    try (MockRM rm = new MockRM(
        getCSConfWithLabelsParentUseWeightChildUsePct(conf))) {
      rm.start();
      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

      String capacityOrWeightString = ((ParentQueue) cs.getQueue(A))
          .getCapacityOrWeightString();
      validateCapacityOrWeightString(capacityOrWeightString, true);

      capacityOrWeightString = ((LeafQueue) cs.getQueue(A1))
          .getCapacityOrWeightString();
      validateCapacityOrWeightString(capacityOrWeightString, false);

      capacityOrWeightString = ((LeafQueue) cs.getQueue(A1))
          .getExtendedCapacityOrWeightString();
      validateCapacityOrWeightString(capacityOrWeightString, false);
    }
  }

  /**
   * This test ensures that while iterating through a parent's Node Labels
   * (when calculating the normalized weights) the parent's Node Labels won't
   * be added to the children with weight -1. If the parent
   * has a node label that a specific child doesn't the normalized calling the
   * normalized weight setter will be skipped. The queue root.b has access to
   * the labels "x" and "y", but root.b.b1 won't. For more information see
   * YARN-10807.
   * @throws Exception
   */
  @Test
  void testChildAccessibleNodeLabelsWeightMode() throws Exception {
    MockRM rm = new MockRM(getCSConfWithQueueLabelsWeightOnly(conf));
    rm.start();

    CapacityScheduler cs =
        (CapacityScheduler) rm.getRMContext().getScheduler();
    LeafQueue b1 = (LeafQueue) cs.getQueue(B1);

    Assertions.assertNotNull(b1);
    Assertions.assertTrue(b1.getAccessibleNodeLabels().isEmpty());

    Set<String> b1ExistingNodeLabels = ((CSQueue) b1).getQueueCapacities()
        .getExistingNodeLabels();
    Assertions.assertEquals(1, b1ExistingNodeLabels.size());
    Assertions.assertEquals("", b1ExistingNodeLabels.iterator().next());

    rm.close();
  }

  /**
   * Tests whether weight is correctly reset to -1. See YARN-11016 for further details.
   * @throws IOException if reinitialization fails
   */
  @Test()
  public void testAccessibleNodeLabelsInheritanceNoWeightMode() throws IOException {
    CapacitySchedulerConfiguration newConf = getConfigWithInheritedAccessibleNodeLabel(conf);

    MockRM rm = new MockRM(newConf);
    CapacityScheduler cs =
        (CapacityScheduler) rm.getRMContext().getScheduler();

    Resource clusterResource = Resource.newInstance(1024, 2);
    cs.getRootQueue().updateClusterResource(clusterResource, new ResourceLimits(clusterResource));

    try {
      cs.reinitialize(newConf, rm.getRMContext());
    } catch (Exception e) {
      Assertions.fail("Reinitialization failed with " + e);
    }
  }

  @Test
  void testQueueInfoWeight() throws Exception {
    MockRM rm = new MockRM(conf);
    rm.init(conf);
    rm.start();

    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(
        conf);
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] {"a", "b", "default"});
    csConf.setNonLabeledQueueWeight("root.a", 1);
    csConf.setNonLabeledQueueWeight("root.b", 2);
    csConf.setNonLabeledQueueWeight("root.default", 3);

    // Check queue info capacity
    CapacityScheduler cs =
        (CapacityScheduler)rm.getRMContext().getScheduler();
    cs.reinitialize(csConf, rm.getRMContext());

    LeafQueue a = (LeafQueue)
        cs.getQueue("root.a");
    Assertions.assertNotNull(a);
    Assertions.assertEquals(a.getQueueInfo(false,
        false).getWeight(), 1e-6,
        a.getQueueCapacities().getWeight());

    LeafQueue b = (LeafQueue)
        cs.getQueue("root.b");
    Assertions.assertNotNull(b);
    Assertions.assertEquals(b.getQueueInfo(false,
        false).getWeight(), 1e-6,
        b.getQueueCapacities().getWeight());
    rm.close();
  }

  private void internalTestContainerAllocationWithNodeLabel(
      Configuration csConf) throws Exception {
    /*
     * Queue structure:
     *                      root (*)
     *                  ________________
     *                 /                \
     *               a x(100%), y(50%)   b y(50%), z(100%)
     *               ________________    ______________
     *              /                   /              \
     *             a1 (x,y)         b1(no)              b2(y,z)
     *               100%                          y = 100%, z = 100%
     *
     * Node structure:
     * h1 : x
     * h2 : y
     * h3 : y
     * h4 : z
     * h5 : NO
     *
     * Total resource:
     * x: 4G
     * y: 6G
     * z: 2G
     * *: 2G
     *
     * Resource of
     * a1: x=4G, y=3G, NO=0.2G
     * b1: NO=0.9G (max=1G)
     * b2: y=3, z=2G, NO=0.9G (max=1G)
     *
     * Each node can only allocate two containers
     */

    // set node -> label
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y", "z"));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0),
        toSet("x"), NodeId.newInstance("h2", 0), toSet("y"),
        NodeId.newInstance("h3", 0), toSet("y"), NodeId.newInstance("h4", 0),
        toSet("z"), NodeId.newInstance("h5", 0),
        RMNodeLabelsManager.EMPTY_STRING_SET));

    // inject node label manager
    MockRM rm1 = new MockRM(csConf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 2048);
    MockNM nm2 = rm1.registerNode("h2:1234", 2048);
    MockNM nm3 = rm1.registerNode("h3:1234", 2048);
    MockNM nm4 = rm1.registerNode("h4:1234", 2048);
    MockNM nm5 = rm1.registerNode("h5:1234", 2048);

    ContainerId containerId;

    // launch an app to queue a1 (label = x), and check all container will
    // be allocated in h1
    MockRMAppSubmissionData data2 =
        MockRMAppSubmissionData.Builder.createWithMemory(1024, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("a1")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data2);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // request a container (label = y). can be allocated on nm2
    am1.allocate("*", 1024, 1, new ArrayList<ContainerId>(), "y");
    containerId =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2L);
    Assertions.assertTrue(rm1.waitForState(nm2, containerId,
        RMContainerState.ALLOCATED));
    checkTaskContainersHost(am1.getApplicationAttemptId(), containerId, rm1,
        "h2");

    // launch an app to queue b1 (label = y), and check all container will
    // be allocated in h5
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(1024, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("b1")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data1);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm5);

    // request a container for AM, will succeed
    // and now b1's queue capacity will be used, cannot allocate more containers
    // (Maximum capacity reached)
    am2.allocate("*", 1024, 1, new ArrayList<ContainerId>());
    containerId = ContainerId.newContainerId(am2.getApplicationAttemptId(), 2);
    Assertions.assertFalse(rm1.waitForState(nm4, containerId,
        RMContainerState.ALLOCATED));
    Assertions.assertFalse(rm1.waitForState(nm5, containerId,
        RMContainerState.ALLOCATED));

    // launch an app to queue b2
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1024, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("b2")
            .withUnmanagedAM(false)
            .build();
    RMApp app3 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am3 = MockRM.launchAndRegisterAM(app3, rm1, nm5);

    // request a container. try to allocate on nm1 (label = x) and nm3 (label =
    // y,z). Will successfully allocate on nm3
    am3.allocate("*", 1024, 1, new ArrayList<ContainerId>(), "y");
    containerId = ContainerId.newContainerId(am3.getApplicationAttemptId(), 2);
    Assertions.assertFalse(rm1.waitForState(nm1, containerId,
        RMContainerState.ALLOCATED));
    Assertions.assertTrue(rm1.waitForState(nm3, containerId,
        RMContainerState.ALLOCATED));
    checkTaskContainersHost(am3.getApplicationAttemptId(), containerId, rm1,
        "h3");

    // try to allocate container (request label = z) on nm4 (label = y,z).
    // Will successfully allocate on nm4 only.
    am3.allocate("*", 1024, 1, new ArrayList<ContainerId>(), "z");
    containerId = ContainerId.newContainerId(am3.getApplicationAttemptId(), 3L);
    Assertions.assertTrue(rm1.waitForState(nm4, containerId,
        RMContainerState.ALLOCATED));
    checkTaskContainersHost(am3.getApplicationAttemptId(), containerId, rm1,
        "h4");

    rm1.close();
  }

  private void checkTaskContainersHost(ApplicationAttemptId attemptId,
      ContainerId containerId, ResourceManager rm, String host) {
    YarnScheduler scheduler = rm.getRMContext().getScheduler();
    SchedulerAppReport appReport = scheduler.getSchedulerAppInfo(attemptId);

    Assertions.assertTrue(appReport.getLiveContainers().size() > 0);
    for (RMContainer c : appReport.getLiveContainers()) {
      if (c.getContainerId().equals(containerId)) {
        Assertions.assertEquals(host, c.getAllocatedNode().getHost());
      }
    }
  }

  private void validateCapacityOrWeightString(String capacityOrWeightString,
      boolean shouldContainWeight) {
    Assertions.assertEquals(shouldContainWeight,
        capacityOrWeightString.contains("weight"));
    Assertions.assertEquals(shouldContainWeight,
        capacityOrWeightString.contains("normalizedWeight"));
    Assertions.assertEquals(!shouldContainWeight,
        capacityOrWeightString.contains("capacity"));

  }
}
