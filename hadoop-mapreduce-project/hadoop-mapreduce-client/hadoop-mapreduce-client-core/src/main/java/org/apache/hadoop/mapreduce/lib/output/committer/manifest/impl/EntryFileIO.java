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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileEntry;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.util.functional.FutureIO;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.util.Preconditions.checkState;

/**
 * Read or write entry file.
 * This can be used to create a simple reader, or to create
 * a writer queue where different threads can queue data for
 * writing.
 * The entry file is a SequenceFile with KV = {NullWritable, FileEntry};
 */
public class EntryFileIO {

  private static final Logger LOG = LoggerFactory.getLogger(
      EntryFileIO.class);

  /** Configuration used to load filesystems. */
  private final Configuration conf;

  /**
   * Constructor.
   * @param conf Configuration used to load filesystems
   */
  public EntryFileIO(final Configuration conf) {
    this.conf = conf;
  }

  /**
   * Create a writer to a local file.
   * @param file file
   * @return the writer
   * @throws IOException failure to create the file
   */
  public SequenceFile.Writer createWriter(File file) throws IOException {
    return createWriter(toPath(file));
  }

  /**
   * Create a writer to a file on any FS.
   * @param path path to write to.
   * @return the writer
   * @throws IOException failure to create the file
   */
  public SequenceFile.Writer createWriter(Path path) throws IOException {
    return SequenceFile.createWriter(conf,
        SequenceFile.Writer.file(path),
        SequenceFile.Writer.keyClass(NullWritable.class),
        SequenceFile.Writer.valueClass(FileEntry.class));
  }


  /**
   * Reader is created with sequential reads.
   * @param file file
   * @return the reader
   * @throws IOException failure to open
   */
  public SequenceFile.Reader createReader(File file) throws IOException {
    return createReader(toPath(file));
  }

  /**
   * Reader is created with sequential reads.
   * @param path path
   * @return the reader
   * @throws IOException failure to open
   */
  public SequenceFile.Reader createReader(Path path) throws IOException {
    return new SequenceFile.Reader(conf,
        SequenceFile.Reader.file(path));
  }

  /**
   * Iterator to retrieve file entries from the sequence file.
   * Closeable: cast and invoke to close the reader.
   * @param reader reader;
   * @return iterator
   */
  public RemoteIterator<FileEntry> iterateOver(SequenceFile.Reader reader) {
    return new EntryIterator(reader);
  }

  /**
   * Create and start an entry writer.
   * @param writer writer
   * @param capacity queue capacity
   * @return the writer.
   */
  public EntryWriter launchEntryWriter(SequenceFile.Writer writer, int capacity) {
    final EntryWriter ew = new EntryWriter(writer, capacity);
    ew.start();
    return ew;
  }

  /**
   * Write a sequence of entries to the writer.
   * @param writer writer
   * @param entries entries
   * @param close close the stream afterwards
   * @return number of entries written
   * @throws IOException write failure.
   */
  public static int write(SequenceFile.Writer writer,
      Collection<FileEntry> entries,
      boolean close)
      throws IOException {
    try {
      for (FileEntry entry : entries) {
        writer.append(NullWritable.get(), entry);
      }
      writer.flush();
    } finally {
      if (close) {
        writer.close();
      }
    }
    return entries.size();
  }


  /**
   * Given a file, create a Path.
   * @param file file
   * @return path to the file
   */
  public static Path toPath(final File file) {
    return new Path(file.toURI());
  }

  /**
   * A Writer thread takes reads from a queue containing
   * list of entries to save; these are serialized via the writer to
   * the output stream.
   * Other threads can queue the file entry lists from loaded manifests
   * for them to be written.
   * The these threads will be blocked when the queue capacity is reached.
   */
  public final class EntryWriter implements Closeable {

    private final SequenceFile.Writer writer;

    private final BlockingQueue<List<FileEntry>> queue;

    /**
     * stop flag.
     */
    private final AtomicBoolean stop = new AtomicBoolean(false);

    private final AtomicBoolean active = new AtomicBoolean(false);

    /**
     * Executor of writes.
     */
    private ExecutorService executor;

    /**
     * Future invoked.
     */
    private Future<Integer> future;

    /**
     * count of file entries saved; only updated in one thread
     * so volatile.
     */
    private final AtomicInteger count = new AtomicInteger();

    /**
     * Any failure caught on the writer thread; this should be
     * raised within the task/job thread as it implies that the
     * entire write has failed.
     */
    private final AtomicReference<IOException> failure = new AtomicReference<>();

    /**
     * Create.
     * @param writer writer
     * @param capacity capacity.
     */
    private EntryWriter(SequenceFile.Writer writer, int capacity) {
      checkState(capacity > 0, "invalid queue capacity %s", capacity);
      this.writer = requireNonNull(writer);
      this.queue = new ArrayBlockingQueue<>(capacity);
    }

    /**
     * Is the writer active?
     * @return true if the processor thread is live
     */
    public boolean isActive() {
      return active.get();
    }

    /**
     * Get count of files processed.
     * @return the count
     */
    public int getCount() {
      return count.get();
    }

    /**
     * Any failure.
     * @return any IOException caught when writing the output
     */
    public IOException getFailure() {
      return failure.get();
    }

    /**
     * Start the thread.
     */
    private void start() {
      checkState(executor == null, "already started");
      active.set(true);
      executor = HadoopExecutors.newSingleThreadExecutor();
      future = executor.submit(this::processor);
      LOG.debug("Started entry writer {}", this);
    }

    /**
     * Add a list of entries to the queue.
     * @param entries entries.
     * @return whether the queue worked.
     */
    public boolean enqueue(List<FileEntry> entries) {
      if (entries.isEmpty()) {
        LOG.debug("ignoring enqueue of empty list");
        // exit fast, but return true.
        return true;
      }
      if (active.get()) {
        try {
          queue.put(entries);
          LOG.debug("Queued {}", entries.size());
          return true;
        } catch (InterruptedException e) {
          Thread.interrupted();
          return false;
        }
      } else {
        LOG.debug("Queue inactive; discarding {} entries", entries.size());
        return false;
      }
    }

    /**
     * Queue and process entries until done.
     * @return count of entries written.
     * @throws UncheckedIOException on write failure
     */
    private int processor() {
      try {
        while (!stop.get()) {
          final List<FileEntry> entries = queue.take();
          if (entries.isEmpty()) {
            // exit list reached.
            LOG.debug("List termination initiated");
            stop.set(true);
          } else {
            LOG.debug("Adding block of {} entries", entries.size());
            for (FileEntry entry : entries) {
              append(entry);
            }
          }
        }
      } catch (InterruptedException e) {
        // assume we are stopped here
        LOG.debug("interrupted", e);
        active.set(false);
        stop.set(true);
      }
      return count.get();
    }

    /**
     * write one entry.
     * @param entry entry to write
     * @throws UncheckedIOException on write failure
     */
    private void append(FileEntry entry) {
      try {
        writer.append(NullWritable.get(), entry);

        final int c = count.incrementAndGet();
        LOG.trace("Added entry #{}: {}", c, entry);
      } catch (IOException e) {
        LOG.debug("Write failure", e);
        failure.set(e);
        throw new UncheckedIOException(e);
      }
    }

    /**
     * Close: stop accepting new writes, wait for queued writes to complete
     * @throws IOException failure closing that writer, or somehow the future
     * raises an IOE which isn't caught for later.
     */

    @Override
    public void close() throws IOException {

      // declare as inactive.
      // this stops queueing more data, but leaves
      // the worker thread still polling and writing.
      if (!active.getAndSet(false)) {
        // already stopped
        return;
      }
      LOG.debug("Shutting down writer");
      // signal queue closure by queue an empty list
      try {
        queue.put(new ArrayList<>());
      } catch (InterruptedException e) {
        Thread.interrupted();
      }
      try {
        // wait for the op to finish.
        int total = FutureIO.awaitFuture(future, 1, TimeUnit.MINUTES);
        LOG.debug("Processed {} files", total);
        executor.shutdown();
      } catch (TimeoutException e) {
        // trouble. force close
        executor.shutdownNow();
        // close the stream
      } finally {
        writer.close();
      }
    }

    /**
     * Raise any IOException caught during execution of the writer thread.
     * @throws IOException if one was caught and saved.
     */
    public void maybeRaiseWriteException() throws IOException {
      final IOException f = failure.get();
      if (f != null) {
        throw f;
      }
    }

    @Override
    public String toString() {
      return "EntryWriter{" +
          "stop=" + stop.get() +
          ", active=" + active.get() +
          ", count=" + count.get() +
          ", queue depth=" + queue.size() +
          ", failure=" + failure +
          '}';
    }
  }

  /**
   * Iterator to retrieve file entries from the sequence file.
   * Closeable; it will close automatically when the last element is read.
   * No thread safety.
   */
  @VisibleForTesting
  static final class EntryIterator implements RemoteIterator<FileEntry>, Closeable {

    private final SequenceFile.Reader reader;

    private FileEntry fetched;

    private boolean closed;

    private int count;

    /**
     * Create an iterator.
     * @param reader the file to read from.
     */
    private EntryIterator(final SequenceFile.Reader reader) {
      this.reader = requireNonNull(reader);
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        closed = true;
        reader.close();
      }
    }

    @Override
    public String toString() {
      return "EntryIterator{" +
          "closed=" + closed +
          ", count=" + count +
          ", fetched=" + fetched +
          '}';
    }

    @Override
    public boolean hasNext() throws IOException {
      return fetched != null || fetchNext();
    }

    /**
     * Fetch the next entry.
     * If there is none, then the reader is closed before `false`
     * is returned.
     * @return true if a record was retrieved.
     * @throws IOException IO failure.
     */
    private boolean fetchNext() throws IOException {
      FileEntry readBack = new FileEntry();
      if (reader.next(NullWritable.get(), readBack)) {
        fetched = readBack;
        count++;
        return true;
      } else {
        fetched = null;
        close();
        return false;
      }
    }

    @Override
    public FileEntry next() throws IOException {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      final FileEntry r = fetched;
      fetched = null;
      return r;
    }

    /**
     * Is the stream closed.
     * @return true if closed.
     */
    public boolean isClosed() {
      return closed;
    }

    int getCount() {
      return count;
    }
  }

}
