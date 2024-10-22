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
package org.apache.hadoop.mapreduce.v2.app.webapp.dao;

import java.util.ArrayList;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;

@XmlRootElement(name = "counterGroup")
@XmlAccessorType(XmlAccessType.FIELD)
public class CounterGroupInfo {

  protected String counterGroupName;
  @XmlElement(name = "counter")
  protected ArrayList<CounterInfo> counter;

  public CounterGroupInfo() {
  }

  public CounterGroupInfo(String name, CounterGroup group, CounterGroup mg,
      CounterGroup rg) {
    this.counterGroupName = name;
    this.counter = new ArrayList<CounterInfo>();

    for (Counter c : group) {
      Counter mc = mg == null ? null : mg.findCounter(c.getName());
      Counter rc = rg == null ? null : rg.findCounter(c.getName());
      CounterInfo cinfo = new CounterInfo(c, mc, rc);
      this.counter.add(cinfo);
    }
  }

}
