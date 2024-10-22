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
package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.api.records.YarnApplicationState;

@XmlRootElement(name = "statItem")
@XmlAccessorType(XmlAccessType.FIELD)
public class StatisticsItemInfo {

  protected YarnApplicationState state;
  protected String type;
  protected long count;

  public StatisticsItemInfo() {
  } // JAXB needs this

  public StatisticsItemInfo(
      YarnApplicationState state, String type, long count) {
    this.state = state;
    this.type = type;
    this.count = count;
  }

  public StatisticsItemInfo(StatisticsItemInfo info) {
    this.state = info.state;
    this.type = info.type;
    this.count = info.count;
  }

  public YarnApplicationState getState() {
    return state;
  }

  public String getType() {
    return type;
  }

  public long getCount() {
    return count;
  }

  public void setCount(long count) {
    this.count = count;
  }
}