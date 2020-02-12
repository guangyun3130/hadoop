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

package org.apache.hadoop.fs.s3a;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * The bucket or other AWS resource is unknown.
 * Why not a subclass of FileNotFoundException?
 * There's too much code which caches an FNFE and infers that the file isn't there;
 * a missing bucket is far more significant.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class UnknownStoreException extends IOException {

  /**
   * The AWS S3 error code used to recognize when a 404 means the bucket is
   * unknown.
   */
  public static final String E_NO_SUCH_BUCKET = "NoSuchBucket";

  /**
   * Constructor.
   * @param message message
   */
  public UnknownStoreException(final String message) {
    this(message, null);
  }

  /**
   * Constructor.
   * @param message message
   * @param cause cause (may be null)
   */
  public UnknownStoreException(final String message, Throwable cause) {
    super(message);
    if (cause != null) {
      initCause(cause);
    }
  }
}
