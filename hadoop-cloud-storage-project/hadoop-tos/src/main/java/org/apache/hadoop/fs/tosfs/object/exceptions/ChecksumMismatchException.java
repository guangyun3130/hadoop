package org.apache.hadoop.fs.tosfs.object.exceptions;

import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class ChecksumMismatchException extends IOException {
  public ChecksumMismatchException(String message) {
    super(message);
  }

  public ChecksumMismatchException(byte[] expected, byte[] actual) {
    this(String.format("Expected checksum is %s while actual checksum is %s",
        StringUtils.byteToHexString(expected), StringUtils.byteToHexString(actual)));
  }
}
