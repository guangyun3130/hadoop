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
package org.apache.hadoop.hdfs.server.federation.store.records;

import org.apache.hadoop.hdfs.server.federation.store.StateStoreUtils;
import org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreBaseImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A mock StateStoreDriver that runs in memory that can force IOExceptions
 * upon demand.
 */
public class MockStateStoreDriver extends StateStoreBaseImpl {
  boolean giveErrors = false;
  boolean initialized = false;
  Map<String, Map<String, BaseRecord>> valueMap = new HashMap<>();

  @Override
  public boolean initDriver() {
    initialized = true;
    return true;
  }

  @Override
  public <T extends BaseRecord> boolean initRecordStorage(String className,
                                                          Class<T> clazz) {
    return true;
  }

  @Override
  public boolean isDriverReady() {
    return initialized;
  }

  @Override
  public void close() throws Exception {
    valueMap.clear();
    initialized = false;
  }

  private void checkErrors() throws IOException {
    if (giveErrors) {
      throw new IOException("Induced errors");
    }
  }

  @Override
  @SuppressWarnings({"rawtypes"})
  public <T extends BaseRecord> QueryResult get(Class<T> clazz) throws IOException {
    checkErrors();
    Map<String, BaseRecord> map = valueMap.get(StateStoreUtils.getRecordName(clazz));
    List<BaseRecord> results = map != null ? new ArrayList<>(map.values()) : new ArrayList<>();
    return new QueryResult<>(results, System.currentTimeMillis());
  }

  @Override
  public <T extends BaseRecord> boolean putAll(List<T> records,
                                               boolean allowUpdate,
                                               boolean errorIfExists)
      throws IOException {
    checkErrors();
    for (T record : records) {
      Map<String, BaseRecord> map =
          valueMap.computeIfAbsent(StateStoreUtils.getRecordName(record.getClass()),
              k -> new HashMap<>());
      String key = record.getPrimaryKey();
      BaseRecord oldRecord = map.get(key);
      if (oldRecord == null || allowUpdate) {
        map.put(key, record);
      } else if (errorIfExists) {
        throw new IOException("Record already exists for " + record.getClass()
            + ": " + key);
      }
    }
    return true;
  }

  @Override
  public <T extends BaseRecord> boolean removeAll(Class<T> clazz) throws IOException {
    checkErrors();
    return valueMap.remove(StateStoreUtils.getRecordName(clazz)) != null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends BaseRecord> int remove(Class<T> clazz,
                                           Query<T> query)
      throws IOException {
    checkErrors();
    int result = 0;
    Map<String, BaseRecord> map =
        valueMap.get(StateStoreUtils.getRecordName(clazz));
    if (map != null) {
      for (Iterator<BaseRecord> itr = map.values().iterator(); itr.hasNext(); ) {
        BaseRecord record = itr.next();
        if (query.matches((T) record)) {
          itr.remove();
          result += 1;
        }
      }
    }
    return result;
  }
}
