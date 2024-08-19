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

package org.apache.hadoop.fs.tosfs.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.thirdparty.com.google.common.base.Splitter;
import org.apache.hadoop.util.Preconditions;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ParseUtils {
  private static final String ERROR_MSG = "Failed to parse value %s as %s, property key %s";

  private ParseUtils() {
  }

  public static String envAsString(String key) {
    return envAsString(key, true);
  }

  public static String envAsString(String key, boolean allowNull) {
    String value = System.getenv(key);
    if (!allowNull) {
      Preconditions.checkNotNull(value, "os env key: %s cannot be null", key);
    }
    return value;
  }

  public static String envAsString(String key, String defaultValue) {
    String value = System.getenv(key);
    return StringUtils.isEmpty(value) ? defaultValue : value;
  }

  public static boolean envAsBoolean(String key, boolean defaultValue) {
    String value = System.getenv(key);
    if (StringUtils.isEmpty(value)) {
      return defaultValue;
    }
    checkBoolean(key, value);
    return Boolean.parseBoolean(value);
  }

  public static int getInt(Map<String, String> props, String key) {
    String value = props.get(key);
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(String.format(ERROR_MSG, value, "integer", key));
    }
  }

  public static int getInt(Map<String, String> props, String key, int defaultValue) {
    if (!props.containsKey(key)) {
      return defaultValue;
    } else {
      return getInt(props, key);
    }
  }

  public static long getLong(Map<String, String> props, String key) {
    String value = props.get(key);
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(String.format(ERROR_MSG, value, "long", key));
    }
  }

  public static long getLong(Map<String, String> props, String key, long defaultValue) {
    if (!props.containsKey(key)) {
      return defaultValue;
    } else {
      return getLong(props, key);
    }
  }

  public static double getDouble(Map<String, String> props, String key) {
    String value = props.get(key);
    try {
      return Double.parseDouble(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(String.format(ERROR_MSG, value, "double", key));
    }
  }

  public static double getDouble(Map<String, String> props, String key, double defaultValue) {
    if (!props.containsKey(key)) {
      return defaultValue;
    } else {
      return getDouble(props, key);
    }
  }

  public static String getString(Map<String, String> props, String key) {
    String value = props.get(key);
    Preconditions.checkNotNull(value, "The value of config key %s is null", key);
    return value;
  }

  public static String getString(Map<String, String> props, String key, String defaultValue) {
    if (!props.containsKey(key)) {
      return defaultValue;
    } else {
      return getString(props, key);
    }
  }

  public static List<String> getList(Map<String, String> props, String key) {
    String value = props.get(key);
    Preconditions.checkNotNull(value, "The value of config key %s is null", key);
    return Splitter.on(',').splitToStream(value).map(String::trim).collect(Collectors.toList());
  }

  public static List<String> getList(Map<String, String> props, String key, List<String> defaultValue) {
    if (!props.containsKey(key)) {
      return defaultValue;
    } else {
      return getList(props, key);
    }
  }

  public static boolean getBoolean(Map<String, String> props, String key) {
    String value = props.get(key);
    checkBoolean(key, value);
    return Boolean.parseBoolean(value);
  }

  public static boolean getBoolean(Map<String, String> props, String key, boolean defaultValue) {
    if (!props.containsKey(key)) {
      return defaultValue;
    } else {
      return getBoolean(props, key);
    }
  }

  public static boolean isBoolean(String value) {
    return "true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value);
  }

  public static void checkBoolean(String key, String value) {
    if (!isBoolean(value)) {
      throw new IllegalArgumentException(String.format(ERROR_MSG, value, "boolean", key));
    }
  }

  public static boolean isLong(String value) {
    try {
      Long.parseLong(value);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  public static boolean isDouble(String value) {
    try {
      Double.parseDouble(value);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }
}
