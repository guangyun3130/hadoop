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

package org.apache.hadoop.fs.tosfs.conf;

public class ConfKeys {

  public static final ArgumentKey FS_TOS_ENDPOINT = new ArgumentKey("fs.%s.endpoint");

  /**
   * The object storage implementation for the defined scheme. For example, we can delegate the
   * scheme 'abc' to TOS (or other object storage),and access the TOS object storage as
   * 'abc://bucket/path/to/key'
   */
  public static final ArgumentKey FS_OBJECT_STORAGE_IMPL =
      new ArgumentKey("fs.objectstorage.%s.impl");
}
