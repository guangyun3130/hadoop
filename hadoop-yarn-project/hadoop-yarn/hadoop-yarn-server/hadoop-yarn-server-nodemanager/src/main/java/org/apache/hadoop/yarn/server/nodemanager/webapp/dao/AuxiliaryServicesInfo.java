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
package org.apache.hadoop.yarn.server.nodemanager.webapp.dao;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.records.AuxServiceRecord;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.Collection;

/**
 * A list of loaded auxiliary services.
 */
@XmlRootElement(name = "services")
@XmlAccessorType(XmlAccessType.FIELD)
public class AuxiliaryServicesInfo {
  private ArrayList<AuxiliaryServiceInfo> services = new
      ArrayList<>();

  public AuxiliaryServicesInfo() {
    // JAXB needs this
  }

  public void add(AuxServiceRecord s) {
    services.add(new AuxiliaryServiceInfo(s.getName(), s.getVersion(), s
        .getLaunchTime()));
  }

  public void addAll(Collection<AuxServiceRecord> serviceList) {
    for (AuxServiceRecord service : serviceList) {
      add(service);
    }
  }

  public ArrayList<AuxiliaryServiceInfo> getServices() {
    return services;
  }
}
