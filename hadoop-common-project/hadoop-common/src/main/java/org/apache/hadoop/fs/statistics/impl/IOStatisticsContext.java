/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.statistics.impl;

import org.apache.hadoop.fs.statistics.IOStatisticsAggregator;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;

/**
 * An interface defined to capture thread-level IOStatistics by using per
 * thread context consisting of IOStatisticsSnapshot thread map for each
 * worker thread.
 * EmptyIOStatisticsSource is returned as an aggregator if this feature is
 * disabled, resulting in a no-op in aggregation.
 */
public interface IOStatisticsContext {

  /**
   * Get the current thread's IOStatisticsContext.
   *
   * @return instance of IOStatisticsContext for the current thread.
   */
  IOStatisticsContext getCurrentIOStatisticsContext();

  /**
   * Capture the snapshot of current thread's IOStatistics.
   *
   * @return IOStatisticsSnapshot for current thread.
   */
  IOStatisticsSnapshot snapshot();

  /**
   * Get the IOStatisticsAggregator for the current thread.
   *
   * @return return the aggregator for current thread.
   */
  IOStatisticsAggregator getThreadIOStatisticsAggregator();

  /**
   * Reset the current thread's IOStatistics.
   */
  void reset();
}
