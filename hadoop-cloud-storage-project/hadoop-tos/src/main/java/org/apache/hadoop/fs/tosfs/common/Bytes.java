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

import org.apache.hadoop.util.Preconditions;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Bytes {
  private Bytes() {
  }

  public static final byte[] EMPTY_BYTES = new byte[0];

  // Encode basic Java types into big-endian binaries.

  public static byte[] toBytes(boolean b) {
    return new byte[] { b ? (byte) -1 : (byte) 0 };
  }

  public static byte[] toBytes(byte b) {
    return new byte[] { b };
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

  public static byte[] toBytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  // Decode big-endian binaries into basic Java types.

  public static boolean toBoolean(byte[] b) {
    return toBoolean(b, 0, 1);
  }

  public static boolean toBoolean(byte[] b, int off) {
    return toBoolean(b, off, 1);
  }

  public static boolean toBoolean(byte[] b, int off, int len) {
    Preconditions.checkArgument(len == 1, "Invalid len: %s", len);
    return b[off] != (byte) 0;
  }

  public static byte toByte(byte[] b) {
    return b[0];
  }

  public static byte toByte(byte[] b, int off) {
    return b[off];
  }

  public static short toShort(byte[] b) {
    return toShort(b, 0, 2);
  }

  public static short toShort(byte[] b, int off) {
    return toShort(b, off, 2);
  }

  public static short toShort(byte[] b, int off, int len) {
    Preconditions.checkArgument(len == 2, "Invalid len: %s", len);
    Preconditions.checkArgument(off >= 0 && off + len <= b.length,
        "Invalid off: %s, len: %s, array size: %s", off, len, b.length);
    short n = 0;
    n = (short) ((n ^ b[off]) & 0xFF);
    n = (short) (n << 8);
    n ^= (short) (b[off + 1] & 0xFF);
    return n;
  }

  public static int toInt(byte[] b) {
    return toInt(b, 0, 4);
  }

  public static int toInt(byte[] b, int off) {
    return toInt(b, off, 4);
  }

  public static int toInt(byte[] b, int off, int len) {
    Preconditions.checkArgument(len == 4, "Invalid len: %s", len);
    Preconditions.checkArgument(off >= 0 && off + len <= b.length,
        "Invalid off: %s, len: %s, array size: %s", off, len, b.length);
    int n = 0;
    for (int i = off; i < (off + len); i++) {
      n <<= 8;
      n ^= b[i] & 0xFF;
    }
    return n;
  }

  public static int toInt(ByteBuffer b) {
    Preconditions.checkArgument(4 <= b.remaining(),
        "Invalid ByteBuffer which remaining must be >= 4, but is: %s", b.remaining());
    int n = 0;
    int off = b.position();
    for (int i = off; i < (off + 4); i++) {
      n <<= 8;
      n ^= b.get(i) & 0xFF;
    }
    return n;
  }

  public static long toLong(byte[] b) {
    return toLong(b, 0, 8);
  }

  public static long toLong(byte[] b, int off) {
    return toLong(b, off, 8);
  }

  public static long toLong(byte[] b, int off, int len) {
    Preconditions.checkArgument(len == 8, "Invalid len: %s", len);
    Preconditions.checkArgument(off >= 0 && off + len <= b.length,
        "Invalid off: %s, len: %s, array size: %s", off, len, b.length);
    long l = 0;
    for (int i = off; i < off + len; i++) {
      l <<= 8;
      l ^= b[i] & 0xFF;
    }
    return l;
  }

  public static byte[] toBytes(byte[] b, int off, int len) {
    Preconditions.checkArgument(off >= 0, "off %s must be >=0", off);
    Preconditions.checkArgument(len >= 0, "len %s must be >= 0", len);
    Preconditions.checkArgument(off + len <= b.length, "off (%s) + len (%s) must be <= %s", off,
        len, b.length);
    byte[] data = new byte[len];
    System.arraycopy(b, off, data, 0, len);
    return data;
  }

  public static String toString(byte[] b) {
    return new String(b, StandardCharsets.UTF_8);
  }

  public static String toString(byte[] b, int off, int len) {
    return new String(b, off, len, StandardCharsets.UTF_8);
  }
}
