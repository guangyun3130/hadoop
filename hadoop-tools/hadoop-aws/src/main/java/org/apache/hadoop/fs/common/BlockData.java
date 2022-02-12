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

/**
 * Holds information about blocks of data in a file.
 */
public class BlockData {
  // State of each block of data.
  enum State {
    // Data is not yet ready to be read from this block (still being prefetched).
    NOT_READY,

    // A read of this block has been enqueued in the prefetch queue.
    QUEUED,

    // This block is ready to be read. That is, it has been fully read.
    READY,

    // This block has been cached in the local disk cache.
    CACHED
  }

  // State of all blocks in a file.
  private State[] state;

  // The size of a file.
  private final long fileSize;

  // The file is divided into blocks of this size.
  private final int blockSize;

  // The file has these many blocks.
  private final int numBlocks;

  /**
   * Constructs an instance of {@link BlockData}.
   *
   * @param fileSize the size of a file.
   * @param blockSize the file is divided into blocks of this size.
   *
   * @throws IllegalArgumentException if fileSize is negative.
   * @throws IllegalArgumentException if blockSize is negative.
   * @throws IllegalArgumentException if blockSize is zero or negative.
   */
  public BlockData(long fileSize, int blockSize) {
    Validate.checkNotNegative(fileSize, "fileSize");
    if (fileSize == 0) {
      Validate.checkNotNegative(blockSize, "blockSize");
    } else {
      Validate.checkPositiveInteger(blockSize, "blockSize");
    }

    this.fileSize = fileSize;
    this.blockSize = blockSize;
    this.numBlocks =
        (fileSize == 0) ? 0 : ((int) (fileSize / blockSize)) + (fileSize % blockSize > 0 ? 1 : 0);
    this.state = new State[this.numBlocks];
    for (int b = 0; b < this.numBlocks; b++) {
      this.setState(b, State.NOT_READY);
    }
  }

  /**
   * Gets the size of each block.
   *
   * @return the size of each block.
   */
  public int getBlockSize() {
    return this.blockSize;
  }

  /**
   * Gets the size of the associated file.
   *
   * @return the size of the associated file.
   */
  public long getFileSize() {
    return this.fileSize;
  }

  /**
   * Gets the number of blocks in the associated file.
   *
   * @return the number of blocks in the associated file.
   */
  public int getNumBlocks() {
    return this.numBlocks;
  }

  /**
   * Indicates whether the given block is the last block in the associated file.
   *
   * @param blockNumber the id of the desired block.
   * @return true if the given block is the last block in the associated file, false otherwise.
   *
   * @throws IllegalArgumentException if blockNumber is invalid.
   */
  public boolean isLastBlock(int blockNumber) {
    if (this.fileSize == 0) {
      return false;
    }

    throwIfInvalidBlockNumber(blockNumber);

    return blockNumber == (this.numBlocks - 1);
  }

  /**
   * Gets the id of the block that contains the given absolute offset.
   *
   * @param offset the absolute offset to check.
   * @return the id of the block that contains the given absolute offset.
   *
   * @throws IllegalArgumentException if offset is invalid.
   */
  public int getBlockNumber(long offset) {
    throwIfInvalidOffset(offset);

    return (int) (offset / this.blockSize);
  }

  /**
   * Gets the size of the given block.
   *
   * @param blockNumber the id of the desired block.
   * @return the size of the given block.
   */
  public int getSize(int blockNumber) {
    if (this.fileSize == 0) {
      return 0;
    }

    if (this.isLastBlock(blockNumber)) {
      return (int) (this.fileSize - (((long) this.blockSize) * (this.numBlocks - 1)));
    } else {
      return this.blockSize;
    }
  }

  /**
   * Indicates whether the given absolute offset is valid.
   *
   * @param offset absolute offset in the file..
   * @return true if the given absolute offset is valid, false otherwise.
   */
  public boolean isValidOffset(long offset) {
    return (offset >= 0) && (offset < this.fileSize);
  }

  /**
   * Gets the start offset of the given block.

   * @param blockNumber the id of the given block.
   * @return the start offset of the given block.
   *
   * @throws IllegalArgumentException if blockNumber is invalid.
   */
  public long getStartOffset(int blockNumber) {
    throwIfInvalidBlockNumber(blockNumber);

    return blockNumber * (long) this.blockSize;
  }

  /**
   * Gets the relative offset corresponding to the given block and the absolute offset.
   *
   * @param blockNumber the id of the given block.
   * @param offset absolute offset in the file.
   * @return the relative offset corresponding to the given block and the absolute offset.
   *
   * @throws IllegalArgumentException if either blockNumber or offset is invalid.
   */
  public int getRelativeOffset(int blockNumber, long offset) {
    throwIfInvalidOffset(offset);

    return (int) (offset - this.getStartOffset(blockNumber));
  }

  /**
   * Gets the state of the given block.
   *
   * @param blockNumber the id of the given block.
   * @return the state of the given block.
   *
   * @throws IllegalArgumentException if blockNumber is invalid.
   */
  public State getState(int blockNumber) {
    throwIfInvalidBlockNumber(blockNumber);

    return this.state[blockNumber];
  }

  /**
   * Sets the state of the given block to the given value.
   *
   * @param blockNumber the id of the given block.
   * @param blockState the target state.
   *
   * @throws IllegalArgumentException if blockNumber is invalid.
   */
  public void setState(int blockNumber, State blockState) {
    throwIfInvalidBlockNumber(blockNumber);

    this.state[blockNumber] = blockState;
  }

  // Debug helper.
  public String getStateString() {
    StringBuilder sb = new StringBuilder();
    int blockNumber = 0;
    while (blockNumber < this.numBlocks) {
      State tstate = this.getState(blockNumber);
      int endBlockNumber = blockNumber;
      while ((endBlockNumber < this.numBlocks) && (this.getState(endBlockNumber) == tstate)) {
        endBlockNumber++;
      }
      sb.append(String.format("[%03d ~ %03d] %s%n", blockNumber, endBlockNumber - 1, tstate));
      blockNumber = endBlockNumber;
    }
    return sb.toString();
  }

  private void throwIfInvalidBlockNumber(int blockNumber) {
    Validate.checkWithinRange(blockNumber, "blockNumber", 0, this.numBlocks - 1);
  }

  private void throwIfInvalidOffset(long offset) {
    Validate.checkWithinRange(offset, "offset", 0, this.fileSize - 1);
  }
}
