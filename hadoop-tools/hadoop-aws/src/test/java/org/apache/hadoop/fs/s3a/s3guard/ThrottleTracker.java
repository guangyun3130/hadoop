/*
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

package org.apache.hadoop.fs.s3a.s3guard;

import org.junit.Assert;

/**
 * Something to track throttles in DynamoDB metastores.
 * The constructor sets the counters to the current count in the
 * DDB table; a call to {@link #reset()} will set it to the latest values.
 * The {@link #probe()} will pick up the latest values to compare them with
 * the original counts.
 * <p>
 * The toString value logs the state.
 */
class ThrottleTracker {

  private final DynamoDBMetadataStore ddbms;

  private long writeThrottleEventOrig = 0;

  private long readThrottleEventOrig = 0;

  private long batchWriteThrottleCountOrig = 0;

  private long readThrottles;

  private long writeThrottles;

  private long batchThrottles;

  public ThrottleTracker(final DynamoDBMetadataStore ddbms) {
    this.ddbms = ddbms;
    reset();
  }

  /**
   * Reset the counters.
   */
  public synchronized void reset() {
    writeThrottleEventOrig
        = ddbms.getWriteThrottleEventCount();

    readThrottleEventOrig
        = ddbms.getReadThrottleEventCount();

    batchWriteThrottleCountOrig
        = ddbms.getBatchWriteCapacityExceededCount();
  }

  /**
   * Update the latest throttle count; synchronized.
   * @return true if throttling has been detected.
   */
  public synchronized boolean probe() {
    setReadThrottles(
        ddbms.getReadThrottleEventCount() - readThrottleEventOrig);
    setWriteThrottles(ddbms.getWriteThrottleEventCount()
        - writeThrottleEventOrig);
    setBatchThrottles(ddbms.getBatchWriteCapacityExceededCount()
        - batchWriteThrottleCountOrig);
    return isThrottlingDetected();
  }

  @Override
  public String toString() {
    return String.format(
        "Tracker with read throttle events = %d;"
            + " write events = %d;"
            + " batch throttles = %d",
        getReadThrottles(), getWriteThrottles(), getBatchThrottles());
  }

  /**
   * Assert that throttling has been detected.
   */
  public void assertThrottlingDetected() {
    Assert.assertTrue("No throttling detected in " + this +
            " against " + ddbms.toString(),
        isThrottlingDetected());
  }

  /**
   * Has there been any throttling on an operation?
   * @return true iff read, write or batch operations were throttled.
   */
  public boolean isThrottlingDetected() {
    return getReadThrottles() > 0 || getWriteThrottles()
        > 0 || getBatchThrottles() > 0;
  }

  public long getReadThrottles() {
    return readThrottles;
  }

  public void setReadThrottles(long readThrottles) {
    this.readThrottles = readThrottles;
  }

  public long getWriteThrottles() {
    return writeThrottles;
  }

  public void setWriteThrottles(long writeThrottles) {
    this.writeThrottles = writeThrottles;
  }

  public long getBatchThrottles() {
    return batchThrottles;
  }

  public void setBatchThrottles(long batchThrottles) {
    this.batchThrottles = batchThrottles;
  }
}
