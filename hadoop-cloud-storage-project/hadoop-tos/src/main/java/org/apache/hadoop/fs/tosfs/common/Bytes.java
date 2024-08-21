/*
 * ByteDance Volcengine EMR, Copyright 2022.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.tosfs.common;

// TODO: Remove this class?
public class Bytes {
  private Bytes() {
  }

  public static final byte[] EMPTY_BYTES = new byte[0];

  // Encode basic Java types into big-endian binaries.

  public static byte[] toBytes(boolean b) {
    return new byte[]{b ? (byte) -1 : (byte) 0};
  }

  public static byte[] toBytes(byte b) {
    return new byte[]{b};
  }

  public static byte[] toBytes(short val) {
    byte[] b = new byte[2];
    for (int i = 1; i >= 0; i--) {
      b[i] = (byte) val;
      val >>>= 8;
    }
    return b;
  }

  public static byte[] toBytes(int val) {
    byte[] b = new byte[4];
    for (int i = 3; i >= 0; i--) {
      b[i] = (byte) val;
      val >>>= 8;
    }
    return b;
  }

  public static byte[] toBytes(long val) {
    byte[] b = new byte[8];
    for (int i = 7; i >= 0; i--) {
      b[i] = (byte) val;
      val >>>= 8;
    }
    return b;
  }
}
