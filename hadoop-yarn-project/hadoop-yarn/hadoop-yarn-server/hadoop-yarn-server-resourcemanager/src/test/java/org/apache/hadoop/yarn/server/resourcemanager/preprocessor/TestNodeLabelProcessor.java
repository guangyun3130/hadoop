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

package org.apache.hadoop.yarn.server.resourcemanager.preprocessor;


import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

/**
 * This class will test the functionality of NodeLabelProcessor.
 */
public class TestNodeLabelProcessor {

  @Test
  void testNodeLabelProcessor() {
    ContextProcessor nodeLabelProcessor = new NodeLabelProcessor();
    ApplicationId app = ApplicationId.newInstance(123456, 111);
    ApplicationSubmissionContext applicationSubmissionContext =
        mock(ApplicationSubmissionContext.class);
    when(applicationSubmissionContext.getApplicationId()).thenReturn(app);
    nodeLabelProcessor.process("host.cluster2.com", "foo", app,
        applicationSubmissionContext);
    verify(applicationSubmissionContext, times(1))
        .setNodeLabelExpression("foo");
  }
}
