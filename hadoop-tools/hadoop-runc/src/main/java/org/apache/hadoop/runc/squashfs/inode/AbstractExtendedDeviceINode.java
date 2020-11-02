/**
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

package org.apache.hadoop.runc.squashfs.inode;

import org.apache.hadoop.runc.squashfs.SquashFsException;
import org.apache.hadoop.runc.squashfs.metadata.MetadataWriter;
import org.apache.hadoop.runc.squashfs.superblock.SuperBlock;

import java.io.DataInput;
import java.io.IOException;

import static org.apache.hadoop.runc.squashfs.util.BinUtils.DumpOptions.DECIMAL;
import static org.apache.hadoop.runc.squashfs.util.BinUtils.DumpOptions.UNSIGNED;
import static org.apache.hadoop.runc.squashfs.util.BinUtils.dumpBin;

abstract public class AbstractExtendedDeviceINode extends AbstractINode
    implements ExtendedDeviceINode {

  int nlink = 1;
  int device;
  int xattrIndex = XATTR_NOT_PRESENT;

  @Override
  public final int getNlink() {
    return nlink;
  }

  @Override
  public void setNlink(int nlink) {
    this.nlink = nlink;
  }

  @Override
  public final int getDevice() {
    return device;
  }

  @Override
  public void setDevice(int device) {
    this.device = device;
  }

  @Override
  public int getXattrIndex() {
    return xattrIndex;
  }

  @Override
  public void setXattrIndex(int xattrIndex) {
    this.xattrIndex = xattrIndex;
  }

  @Override
  public boolean isXattrPresent() {
    return xattrIndex != XATTR_NOT_PRESENT;
  }

  @Override
  protected final int getChildSerializedSize() {
    return 12;
  }

  @Override
  protected final void readExtraData(SuperBlock sb, DataInput in)
      throws SquashFsException, IOException {
    nlink = in.readInt();
    device = in.readInt();
    xattrIndex = in.readInt();
  }

  @Override
  protected void writeExtraData(MetadataWriter out) throws IOException {
    out.writeInt(nlink);
    out.writeInt(device);
    out.writeInt(xattrIndex);
  }

  @Override
  protected final int getPreferredDumpWidth() {
    return 12;
  }

  @Override
  protected final void dumpProperties(StringBuilder buf, int width) {
    dumpBin(buf, width, "nlink", nlink, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "device", device, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "xattrIndex", xattrIndex, DECIMAL, UNSIGNED);
  }

}
