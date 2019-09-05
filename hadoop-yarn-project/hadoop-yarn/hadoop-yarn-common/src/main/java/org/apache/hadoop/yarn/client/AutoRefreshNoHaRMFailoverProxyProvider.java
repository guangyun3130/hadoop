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

package org.apache.hadoop.yarn.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * An subclass of {@link RMFailoverProxyProvider} which try to
 * resolve the proxy DNS in the event of failover.
 * This provider don't support HA or Federation.
 */
public class AutoRefreshNoHARMFailoverProxyProvider<T>
    extends DefaultNoHARMFailoverProxyProvider<T> {

  private static final Logger LOG =
    LoggerFactory.getLogger(AutoRefreshNoHARMFailoverProxyProvider.class);
  protected RMProxy<T> rmProxy;
  protected YarnConfiguration conf;

  @Override
  public void init(Configuration configuration, RMProxy<T> rmProxy,
      Class<T> protocol) {
    this.rmProxy = rmProxy;
    this.protocol = protocol;
    this.conf = new YarnConfiguration(configuration);
  }

  /**
   * Create or get the proxy from RMProxy.
   * @return
   */
  @Override
  public synchronized ProxyInfo<T> getProxy() {
    if (proxy == null) {
      proxy = getProxyInternal();
    }
    return new ProxyInfo<T>(proxy, null);
  }

  private T getProxyInternal() {
    try {
      final InetSocketAddress rmAddress = rmProxy.getRMAddress(conf, protocol);
      return rmProxy.getProxy(conf, protocol, rmAddress);
    } catch (IOException ioe) {
      LOG.error("Unable to create proxy to the ResourceManager ", ioe);
      return null;
    }
  }

  /**
   * Stop the current proxy when performFailover.
   * @param currentProxy
   */
  @Override
  public synchronized void performFailover(T currentProxy) {
    RPC.stopProxy(proxy);
    proxy = null;
  }
}
