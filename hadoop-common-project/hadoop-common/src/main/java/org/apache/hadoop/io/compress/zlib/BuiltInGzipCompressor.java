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

package org.apache.hadoop.io.compress.zlib;

import java.io.IOException;
import java.util.zip.Checksum;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.DoNotPool;
import org.apache.hadoop.util.DataChecksum;

/**
 * A {@link Compressor} based on the popular gzip compressed file format.
 * http://www.gzip.org/
 */
@DoNotPool
public class BuiltInGzipCompressor implements Compressor {

    /**
     * Fixed ten-byte gzip header. See {@link GZIPOutputStream}'s source for
     * details.
     */
    private static final byte[] GZIP_HEADER = new byte[] {
            0x1f, (byte) 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };

    private Deflater deflater;

    private int headerOff = 0;

    private byte[] userBuf = null;
    private int userBufOff = 0;
    private int userBufLen = 0;

    private int headerBytesWritten = 0;
    private int trailerBytesWritten = 0;

    private int currentInputLen = 0;

    private Checksum crc = DataChecksum.newCrc32();

    private BuiltInGzipDecompressor.GzipStateLabel state;

    public BuiltInGzipCompressor(Configuration conf) {
        ZlibCompressor.CompressionLevel level = ZlibFactory.getCompressionLevel(conf);
        ZlibCompressor.CompressionStrategy strategy = ZlibFactory.getCompressionStrategy(conf);

        // 'true' (nowrap) => Deflater will handle raw deflate stream only
        deflater = new Deflater(level.compressionLevel(), true);
        deflater.setStrategy(strategy.compressionStrategy());

        state = BuiltInGzipDecompressor.GzipStateLabel.HEADER_BASIC;
        crc.reset();
    }

    @Override
    public boolean finished() {
        return deflater.finished();
    }

    @Override
    public boolean needsInput() {
        if (state == BuiltInGzipDecompressor.GzipStateLabel.INFLATE_STREAM) {
            return deflater.needsInput();
        }

        return (state != BuiltInGzipDecompressor.GzipStateLabel.FINISHED);
    }

    @Override
    public int compress(byte[] b, int off, int len) throws IOException {
        int numAvailBytes = 0;

        // If we are not within uncompression data yet. Output the header.
        if (state != BuiltInGzipDecompressor.GzipStateLabel.INFLATE_STREAM) {
            if (userBufLen <= 0) {
                return numAvailBytes;
            }

            int outputHeaderSize = writeHeader(b, off, len);

            // Completes header output.
            if (headerOff == 10) {
                state = BuiltInGzipDecompressor.GzipStateLabel.INFLATE_STREAM;
            }

            numAvailBytes += outputHeaderSize;

            if (outputHeaderSize == len) {
                return numAvailBytes;
            }

            off += outputHeaderSize;
            len -= outputHeaderSize;
        }

        if (state == BuiltInGzipDecompressor.GzipStateLabel.INFLATE_STREAM) {
            // hand off user data (or what's left of it) to Deflater--but note that
            // Deflater may not have consumed all of previous bufferload, in which case
            // userBufLen will be zero
            if (userBufLen > 0) {
                deflater.setInput(userBuf, userBufOff, userBufLen);

                crc.update(userBuf, userBufOff, userBufLen);  // CRC-32 is on uncompressed data

                currentInputLen = userBufLen;
                userBufOff += userBufLen;
                userBufLen = 0;
            }


            // now compress it into b[]
            int deflated = deflater.deflate(b, off, len - 8, Deflater.FULL_FLUSH);

            numAvailBytes += deflated;
            off += deflated;
            len -= deflated;

            // All current input are processed. Going to output trailer.
            if (deflater.finished()) {
                state = BuiltInGzipDecompressor.GzipStateLabel.TRAILER_CRC;
            } else {
                return numAvailBytes;
            }
        }

        numAvailBytes += writeTrailer(b, off, len);

        return numAvailBytes;
    }

    @Override
    public long getBytesRead() {
        return deflater.getTotalIn();
    }

    @Override
    public long getBytesWritten() {
        return headerBytesWritten + deflater.getTotalOut() + trailerBytesWritten;
    }

    @Override
    public void end() { deflater.end(); }

    @Override
    public void finish() { deflater.finish(); }

    @Override
    public void reinit(Configuration conf) {
        ZlibCompressor.CompressionLevel level = ZlibFactory.getCompressionLevel(conf);
        ZlibCompressor.CompressionStrategy strategy = ZlibFactory.getCompressionStrategy(conf);

        // 'true' (nowrap) => Deflater will handle raw deflate stream only
        deflater = new Deflater(level.compressionLevel(), true);
        deflater.setStrategy(strategy.compressionStrategy());
        state = BuiltInGzipDecompressor.GzipStateLabel.HEADER_BASIC;
        crc.reset();
        userBufOff = userBufLen = 0;
        headerBytesWritten = trailerBytesWritten = 0;
        currentInputLen = 0;
        headerOff = 0;
    }

    @Override
    public void reset() {
        deflater.reset();
        state = BuiltInGzipDecompressor.GzipStateLabel.HEADER_BASIC;
        crc.reset();
        userBufOff = userBufLen = 0;
        currentInputLen = 0;
        headerOff = 0;
    }

    @Override
    public void setDictionary(byte[] b, int off, int len) {
        deflater.setDictionary(b, off, len);
    }

    @Override
    public void setInput(byte[] b, int off, int len) {
        if (b == null) {
            throw new NullPointerException();
        }
        if (off < 0 || len < 0 || off > b.length - len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        userBuf = b;
        userBufOff = off;
        userBufLen = len;

        deflater.reset();
        crc.reset();
    }

    private int writeHeader(byte[] b, int off, int len) {
        if (len <= 0) {
            return 0;
        }

        int n = Math.min(len, 10 - headerOff);
        System.arraycopy(GZIP_HEADER, headerOff, b, off, n);
        headerOff += n;

        return n;
    }

    private int writeTrailer(byte[] b, int off, int len) {
        int writtenSize = 0;

        if (len >= 4 && state == BuiltInGzipDecompressor.GzipStateLabel.TRAILER_CRC) {
            int streamCrc = (int) crc.getValue();
            b[off] = (byte) (streamCrc & 0x000000ff);
            b[off + 1] = (byte) ((streamCrc & 0x0000ff00) >> 8);
            b[off + 2] = (byte) ((streamCrc & 0x00ff0000) >> 16);
            b[off + 3] = (byte) ((streamCrc & 0xff000000) >> 24);

            len -= 4;
            off += 4;

            writtenSize += 4;
            state = BuiltInGzipDecompressor.GzipStateLabel.TRAILER_SIZE;
        }

        if (len >= 4 && state == BuiltInGzipDecompressor.GzipStateLabel.TRAILER_SIZE) {
            b[off] = (byte) (currentInputLen & 0x000000ff);
            b[off + 1] = (byte) ((currentInputLen & 0x0000ff00) >> 8);
            b[off + 2] = (byte) ((currentInputLen & 0x00ff0000) >> 16);
            b[off + 3] = (byte) ((currentInputLen & 0xff000000) >> 24);

            len -= 4;
            off += 4;
            writtenSize += 4;

            state = BuiltInGzipDecompressor.GzipStateLabel.FINISHED;
            currentInputLen = 0;
            headerOff = 0;
        }

        return writtenSize;
    }
}
