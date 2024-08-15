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

public class TosKeys {
  /**
   * Tos object storage endpoint to connect to, which should include both region and object domain
   * name.
   */
  public static final String FS_TOS_ENDPOINT = "fs.tos.endpoint";

  /**
   * The accessKey key to access the tos object storage.
   */
  public static final String FS_TOS_ACCESS_KEY_ID = "fs.tos.access-key-id";

  /**
   * The secret access key to access the object storage.
   */
  public static final String FS_TOS_SECRET_ACCESS_KEY = "fs.tos.secret-access-key";

  /**
   * The session token to access the object storage.
   */
  public static final String FS_TOS_SESSION_TOKEN = "fs.tos.session-token";

  /**
   * The access key to access the object storage for the configured bucket, where %s is the bucket
   * name.
   */
  public static final String FS_TOS_BUCKET_ACCESS_KEY_ID_TEMPLATE = "fs.tos.bucket.%s.access-key-id";

  /**
   * The secret access key to access the object storage for the configured bucket, where %s is the
   * bucket name.
   */
  public static final String FS_TOS_BUCKET_SECRET_ACCESS_KEY_TEMPLATE = "fs.tos.bucket.%s.secret-access-key";

  /**
   * The session token to access the object storage for the configured bucket, where %s is the
   * bucket name.
   */
  public static final String FS_TOS_BUCKET_SESSION_TOKEN_TEMPLATE = "fs.tos.bucket.%s.session-token";

  /**
   * User customized credential provider classes, separate provider class name with comma if there
   * are multiple providers.
   */
  public static final String FS_TOS_CUSTOM_CREDENTIAL_PROVIDER_CLASSES =
      "fs.tos.credential.provider.custom.classes";

  public static final String FS_TOS_CUSTOM_CREDENTIAL_PROVIDER_CLASSES_DEFAULT =
      "io.proton.common.object.tos.auth.EnvironmentCredentialsProvider,io.proton.common.object.tos.auth.SimpleCredentialsProvider";

  /**
   * Construct key from template and corresponding arguments.
   */
  public static final String get(String template, Object... arguments) {
    return String.format(template, arguments);
  }
}
