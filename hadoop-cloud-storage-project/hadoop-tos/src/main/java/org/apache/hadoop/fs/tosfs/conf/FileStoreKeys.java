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

public class FileStoreKeys {
  /**
   * File store object storage endpoint to connect to.
   */
  public static final String FS_FILESTORE_ENDPOINT = "fs.filestore.endpoint";

  /**
   * The key indicates the name of the filestore checksum algorithm. Specify the algorithm name to
   * satisfy different storage systems. For example, the hdfs style name is COMPOSITE-CRC32 and
   * COMPOSITE-CRC32C.
   */
  public static final String FS_FILESTORE_CHECKSUM_ALGORITHM = "fs.filestore.checksum-algorithm";

  /**
   * The key indicates how to retrieve file checksum from filestore, error will be thrown if the
   * configured checksum type is not supported. The supported checksum type is: MD5.
   */
  public static final String FS_FILESTORE_CHECKSUM_TYPE = "fs.filestore.checksum-type";

  /**
   * The batch size of deleting multiple objects per request for the given object storage.
   */
  public static final String FS_FILESTORE_BATCH_DELETE_SIZE = "fs.filestore.delete.batch-size";
  public static final int FS_FILESTORE_BATCH_DELETE_SIZE_DEFAULT = 250;
}
