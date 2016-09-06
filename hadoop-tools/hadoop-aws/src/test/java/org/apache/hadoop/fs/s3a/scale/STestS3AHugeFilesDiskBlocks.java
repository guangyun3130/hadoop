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

package org.apache.hadoop.fs.s3a.scale;

import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.fs.s3a.Constants.BLOCK_OUTPUT;
import static org.apache.hadoop.fs.s3a.Constants.BLOCK_OUTPUT_BUFFER;
import static org.apache.hadoop.fs.s3a.Constants.BLOCK_OUTPUT_BUFFER_DISK;
import static org.apache.hadoop.fs.s3a.Constants.FAST_UPLOAD;
import static org.apache.hadoop.fs.s3a.Constants.MIN_MULTIPART_THRESHOLD;
import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_MIN_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_SIZE;

public class STestS3AHugeFilesDiskBlocks extends AbstractSTestS3AHugeFiles {

  @Override
  protected Configuration createConfiguration() {
    final Configuration configuration = super.createConfiguration();
    configuration.setBoolean(BLOCK_OUTPUT, true);
    configuration.set(BLOCK_OUTPUT_BUFFER, BLOCK_OUTPUT_BUFFER_DISK);
    return configuration;
  }
}
