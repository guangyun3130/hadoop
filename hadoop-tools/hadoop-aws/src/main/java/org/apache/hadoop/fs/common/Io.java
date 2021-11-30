/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.common;

import java.io.Closeable;
import java.io.IOException;

/**
 * Provides misc functionality related to IO.
 */
public final class Io {
  private Io() {}

  /**
   * Closes the given resource and ignores any IOException if thrown.
   */
  public static void closeIgnoringIoException(Closeable resource) {
    try {
      if (resource != null) {
        resource.close();
      }
    } catch (IOException e) {
      // Ignored on purpose as there is not much we can do here.
    }
  }
}
