/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.tserver.log;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.accumulo.tserver.log.DfsWalReader.LOG_FILE_HEADER_V4;
import static org.apache.accumulo.tserver.logger.LogEvents.COMPACTION_FINISH;
import static org.apache.accumulo.tserver.logger.LogEvents.COMPACTION_START;
import static org.apache.accumulo.tserver.logger.LogEvents.DEFINE_TABLET;
import static org.apache.accumulo.tserver.logger.LogEvents.MANY_MUTATIONS;
import static org.apache.accumulo.tserver.logger.LogEvents.MUTATION;
import static org.apache.accumulo.tserver.logger.LogEvents.OPEN;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.crypto.CryptoEnvironmentImpl;
import org.apache.accumulo.core.crypto.CryptoUtils;
import org.apache.accumulo.core.crypto.streams.NoFlushOutputStream;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment.Scope;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.spi.crypto.CryptoServiceFactory;
import org.apache.accumulo.core.spi.crypto.FileEncrypter;
import org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment;
import org.apache.accumulo.core.spi.wal.WriteAheadLog;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.Timer;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeChooserEnvironmentImpl;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Wrap a connection to a logger.
 *
 */
public final class DfsLogger implements WriteAheadLog {

  private static final Logger log = LoggerFactory.getLogger(DfsLogger.class);
  private static final DatanodeInfo[] EMPTY_PIPELINE = new DatanodeInfo[0];

  private final LinkedBlockingQueue<LogWork> workQueue = new LinkedBlockingQueue<>();

  private final Object closeLock = new Object();

  private static final LogWork CLOSED_MARKER = new LogWork(null, Durability.FLUSH);

  private static final LogFileValue EMPTY = new LogFileValue();

  private boolean closed = false;

  private class LogSyncingTask implements Runnable {
    private int expectedReplication = 0;

    private final AtomicLong syncCounter;
    private final AtomicLong flushCounter;
    private final Duration slowFlushDuration;

    LogSyncingTask(AtomicLong syncCounter, AtomicLong flushCounter, Duration slowFlushDuration) {
      this.syncCounter = syncCounter;
      this.flushCounter = flushCounter;
      this.slowFlushDuration = slowFlushDuration;
    }

    @Override
    public void run() {
      ArrayList<LogWork> work = new ArrayList<>();
      boolean sawClosedMarker = false;
      while (!sawClosedMarker) {
        work.clear();

        try {
          work.add(workQueue.take());
        } catch (InterruptedException ex) {
          continue;
        }
        workQueue.drainTo(work);

        Optional<Boolean> shouldHSync = Optional.empty();
        loop: for (LogWork logWork : work) {
          switch (logWork.durability) {
            case DEFAULT:
            case NONE:
            case LOG:
              // shouldn't make it to the work queue
              throw new IllegalArgumentException("unexpected durability " + logWork.durability);
            case SYNC:
              shouldHSync = Optional.of(Boolean.TRUE);
              break loop;
            case FLUSH:
              if (shouldHSync.isEmpty()) {
                shouldHSync = Optional.of(Boolean.FALSE);
              }
              break;
          }
        }

        Timer timer = Timer.startNew();
        try {
          if (shouldHSync.isPresent()) {
            if (shouldHSync.orElseThrow()) {
              logFile.hsync();
              syncCounter.incrementAndGet();
            } else {
              logFile.hflush();
              flushCounter.incrementAndGet();
            }
          }
        } catch (IOException | RuntimeException ex) {
          fail(work, ex, "synching");
        }
        if (timer.hasElapsed(slowFlushDuration)) {
          log.info("Slow sync cost: {} ms, current pipeline: {}", timer.elapsed(MILLISECONDS),
              Arrays.toString(getPipeLine()));
          if (expectedReplication > 0) {
            int current = expectedReplication;
            try {
              current = ((DFSOutputStream) logFile.getWrappedStream()).getCurrentBlockReplication();
            } catch (IOException e) {
              fail(work, e, "getting replication level");
            }
            if (current < expectedReplication) {
              fail(work,
                  new IOException(
                      "replication of " + current + " is less than " + expectedReplication),
                  "replication check");
            }
          }
        }
        if (expectedReplication == 0 && logFile.getWrappedStream() instanceof DFSOutputStream) {
          try {
            expectedReplication =
                ((DFSOutputStream) logFile.getWrappedStream()).getCurrentBlockReplication();
          } catch (IOException e) {
            fail(work, e, "getting replication level");
          }
        }

        for (LogWork logWork : work) {
          if (logWork == CLOSED_MARKER) {
            sawClosedMarker = true;
          } else {
            logWork.latch.countDown();
          }
        }
      }
    }

    private void fail(ArrayList<LogWork> work, Exception ex, String why) {
      log.warn("Exception {} {}", why, ex, ex);
      for (LogWork logWork : work) {
        logWork.exception = ex;
      }
    }
  }

  private static class LogWork {
    final CountDownLatch latch;
    final Durability durability;
    volatile Exception exception;

    public LogWork(CountDownLatch latch, Durability durability) {
      this.latch = latch;
      this.durability = durability;
    }
  }

  static class LoggerOperation implements WriteAheadLog.Operation {
    private final LogWork work;

    public LoggerOperation(LogWork work) {
      this.work = work;
    }

    public void await() throws IOException {
      try {
        work.latch.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      if (work.exception != null) {
        if (work.exception instanceof IOException) {
          throw (IOException) work.exception;
        } else if (work.exception instanceof RuntimeException) {
          throw (RuntimeException) work.exception;
        } else {
          throw new RuntimeException(work.exception);
        }
      }
    }
  }

  private static class NoWaitLoggerOperation extends LoggerOperation {

    public NoWaitLoggerOperation() {
      super(null);
    }

    @Override
    public void await() {}
  }

  static final LoggerOperation NO_WAIT_LOGGER_OP = new NoWaitLoggerOperation();

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj instanceof DfsLogger) {
      return logEntry.equals(((DfsLogger) obj).logEntry);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return logEntry.hashCode();
  }

  private FSDataOutputStream logFile;
  private DataOutputStream encryptingLogFile = null;
  private final LogEntry logEntry;
  private Thread syncThread;

  private long writes = 0;

  /**
   * Create a new DfsLogger with the provided characteristics.
   */
  static WriteAheadLog createNew(VolumeManager volumeManager, AccumuloConfiguration conf,
      CryptoServiceFactory cryptoServiceFactory, Set<String> baseUris, AtomicLong syncCounter,
      AtomicLong flushCounter, String address) throws IOException {

    String filename = UUID.randomUUID().toString();
    String addressForFilename = address.replace(':', '+');

    var chooserEnv = new VolumeChooserEnvironmentImpl(VolumeChooserEnvironment.Scope.LOGGER,
        new ServerContext(SiteConfiguration.auto()));
    String logPath = volumeManager.choose(chooserEnv, baseUris) + Path.SEPARATOR + Constants.WAL_DIR
        + Path.SEPARATOR + addressForFilename + Path.SEPARATOR + filename;

    LogEntry log = LogEntry.fromPath(logPath);
    DfsLogger dfsLogger = new DfsLogger(log);
    long slowFlushMillis = conf.getTimeInMillis(Property.TSERV_SLOW_FLUSH_MILLIS);
    dfsLogger.open(volumeManager, conf, cryptoServiceFactory, logPath, filename, address,
        syncCounter, flushCounter, slowFlushMillis);
    return dfsLogger;
  }

  /**
   * Reference a pre-existing log file.
   *
   * @param logEntry the "log" entry in +r/!0
   */
  public static DfsLogger fromLogEntry(LogEntry logEntry) {
    return new DfsLogger(logEntry);
  }

  private DfsLogger(LogEntry logEntry) {
    this.logEntry = logEntry;
  }

  /**
   * Opens a Write-Ahead Log file and writes the necessary header information and OPEN entry to the
   * file. The file is ready to be used for ingest if this method returns successfully. If an
   * exception is thrown from this method, it is the callers responsibility to ensure that
   * {@link #close()} is called to prevent leaking the file handle and/or syncing thread.
   *
   * @param address The address of the host using this WAL
   */
  private synchronized void open(VolumeManager volumeManager, AccumuloConfiguration conf,
      CryptoServiceFactory cryptoServiceFactory, String logPath, String filename, String address,
      AtomicLong syncCounter, AtomicLong flushCounter, long slowFlushMillis) throws IOException {
    log.debug("Address is {}", address);

    log.debug("DfsLogger.open() begin");

    LoggerOperation op;

    try {
      Path logfilePath = new Path(logPath);
      short replication = (short) conf.getCount(Property.TSERV_WAL_REPLICATION);
      if (replication == 0) {
        replication = volumeManager.getDefaultReplication(logfilePath);
      }
      long blockSize = DfsWalReader.computeWalBlockSize(conf);
      if (conf.getBoolean(Property.TSERV_WAL_SYNC)) {
        logFile = volumeManager.createSyncable(logfilePath, 0, replication, blockSize);
      } else {
        logFile = volumeManager.create(logfilePath, true, 0, replication, blockSize);
      }

      // Tell the DataNode that the write ahead log does not need to be cached in the OS page cache
      try {
        logFile.setDropBehind(Boolean.TRUE);
      } catch (UnsupportedOperationException e) {
        log.debug("setDropBehind writes not enabled for wal file: {}", logFile);
      } catch (IOException e) {
        log.debug("IOException setting drop behind for file: {}, msg: {}", logFile, e.getMessage());
      }

      // check again that logfile can be sync'd
      if (!volumeManager.canSyncAndFlush(logfilePath)) {
        log.warn("sync not supported for log file {}. Data loss may occur.", logPath);
      }

      // Initialize the log file with a header and its encryption
      CryptoEnvironment env = new CryptoEnvironmentImpl(Scope.WAL);
      CryptoService cryptoService =
          cryptoServiceFactory.getService(env, conf.getAllCryptoProperties());
      logFile.write(LOG_FILE_HEADER_V4.getBytes(UTF_8));

      log.debug("Using {} for encrypting WAL {}", cryptoService.getClass().getSimpleName(),
          filename);
      FileEncrypter encrypter = cryptoService.getFileEncrypter(env);
      byte[] cryptoParams = encrypter.getDecryptionParameters();
      CryptoUtils.writeParams(cryptoParams, logFile);

      /*
       * Always wrap the WAL in a NoFlushOutputStream to prevent extra flushing to HDFS. The method
       * write(LogFileKey, LogFileValue) will flush crypto data or do nothing when crypto is not
       * enabled.
       */
      OutputStream encryptedStream = encrypter.encryptStream(new NoFlushOutputStream(logFile));
      if (encryptedStream instanceof NoFlushOutputStream) {
        encryptingLogFile = (NoFlushOutputStream) encryptedStream;
      } else {
        encryptingLogFile = new DataOutputStream(encryptedStream);
      }

      LogFileKey key = new LogFileKey();
      key.setEvent(OPEN);
      key.setTserverSession(filename);
      key.setFilename(filename);
      op = logKeyData(key, Durability.SYNC);
    } catch (Exception ex) {
      if (logFile != null) {
        logFile.close();
      }
      logFile = null;
      encryptingLogFile = null;
      throw new IOException(ex);
    }

    syncThread = Threads.createCriticalThread("Accumulo WALog thread " + this,
        new LogSyncingTask(syncCounter, flushCounter, Duration.ofMillis(slowFlushMillis)));
    syncThread.start();
    op.await();
    log.debug("Got new write-ahead log: {}", this);
  }

  @Override
  public String toString() {
    return logEntry.toString();
  }

  public String getLogEntryPath() {
    return logEntry.getPath();
  }

  public Path getPath() {
    return new Path(logEntry.getPath());
  }

  public void close() throws IOException {

    synchronized (closeLock) {
      if (closed) {
        return;
      }
      // after closed is set to true, nothing else should be added to the queue
      // CLOSED_MARKER should be the last thing on the queue, therefore when the
      // background thread sees the marker and exits there should be nothing else
      // to process... so nothing should be left waiting for the background
      // thread to do work
      closed = true;
      workQueue.add(CLOSED_MARKER);
    }

    // wait for background thread to finish before closing log file
    if (syncThread != null) {
      try {
        syncThread.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    // expect workq should be empty at this point
    if (!workQueue.isEmpty()) {
      log.error("WAL work queue not empty after sync thread exited");
      throw new IllegalStateException("WAL work queue not empty after sync thread exited");
    }

    if (encryptingLogFile != null) {
      try {
        logFile.close();
      } catch (IOException ex) {
        log.error("Failed to close log file", ex);
        throw new WriteAheadLog.LogClosedException();
      }
    }
  }

  public synchronized long getWrites() {
    Preconditions.checkState(writes >= 0);
    return writes;
  }

  @Override
  public WriteAheadLog.Operation defineTablet(long seq, int tabletId, TableId tableId, Text endRow,
      Text prevEndRow) throws IOException {
    // write this log to the METADATA table
    final LogFileKey key = new LogFileKey();
    key.setEvent(DEFINE_TABLET);
    key.setSeq(seq);
    key.setTabletId(tabletId);
    key.setTablet(new KeyExtent(tableId, endRow, prevEndRow));
    return logKeyData(key, Durability.LOG);
  }

  private synchronized void write(LogFileKey key, LogFileValue value) throws IOException {
    key.write(encryptingLogFile);
    value.write(encryptingLogFile);
    encryptingLogFile.flush();
    writes++;
  }

  private LoggerOperation logKeyData(LogFileKey key, Durability d) throws IOException {
    return logFileData(singletonList(new Pair<>(key, EMPTY)), d);
  }

  private LoggerOperation logFileData(List<Pair<LogFileKey,LogFileValue>> keys,
      Durability durability) throws IOException {
    LogWork work = new LogWork(new CountDownLatch(1), durability);
    try {
      for (Pair<LogFileKey,LogFileValue> pair : keys) {
        write(pair.getFirst(), pair.getSecond());
      }
    } catch (ClosedChannelException ex) {
      throw new WriteAheadLog.LogClosedException();
    } catch (Exception e) {
      log.error("Failed to write log entries", e);
      work.exception = e;
    }

    synchronized (closeLock) {
      // use a different lock for close check so that adding to work queue does not need
      // to wait on walog I/O operations

      if (closed) {
        throw new WriteAheadLog.LogClosedException();
      }

      if (durability == Durability.LOG) {
        return NO_WAIT_LOGGER_OP;
      }

      workQueue.add(work);
    }

    return new LoggerOperation(work);
  }

  public WriteAheadLog.Operation logManyTablets(Collection<WriteAheadLog.TabletWrite> mutations)
      throws IOException {
    Durability durability = Durability.NONE;
    List<Pair<LogFileKey,LogFileValue>> data = new ArrayList<>();
    for (WriteAheadLog.TabletWrite tabletMutations : mutations) {
      LogFileKey key = new LogFileKey();
      key.setEvent(MANY_MUTATIONS);
      key.setSeq(tabletMutations.getSequence());
      key.setTabletId(tabletMutations.getTabletId());
      LogFileValue value = new LogFileValue();
      value.setMutations(tabletMutations.getMutations());
      data.add(new Pair<>(key, value));
      durability = maxDurability(tabletMutations.getDurability(), durability);
    }
    return logFileData(data, durability);
  }

  public LoggerOperation log(long seq, int tabletId, Mutation m, Durability d) throws IOException {
    LogFileKey key = new LogFileKey();
    key.setEvent(MUTATION);
    key.setSeq(seq);
    key.setTabletId(tabletId);
    LogFileValue value = new LogFileValue();
    value.setMutations(singletonList(m));
    return logFileData(singletonList(new Pair<>(key, value)), d);
  }

  /**
   * Return the Durability with the highest precedence
   */
  static Durability maxDurability(Durability dur1, Durability dur2) {
    if (dur1.ordinal() > dur2.ordinal()) {
      return dur1;
    } else {
      return dur2;
    }
  }

  @Override
  public WriteAheadLog.Operation minorCompactionFinished(long seq, int tabletId,
      Durability durability) throws IOException {
    LogFileKey key = new LogFileKey();
    key.setEvent(COMPACTION_FINISH);
    key.setSeq(seq);
    key.setTabletId(tabletId);
    return logKeyData(key, durability);
  }

  @Override
  public WriteAheadLog.Operation minorCompactionStarted(long seq, int tabletId, String fqfn,
      Durability durability) throws IOException {
    LogFileKey key = new LogFileKey();
    key.setEvent(COMPACTION_START);
    key.setSeq(seq);
    key.setTabletId(tabletId);
    key.setFilename(fqfn);
    return logKeyData(key, durability);
  }

  /*
   * The following method was shamelessly lifted from HBASE-11240 (sans reflection). Thanks HBase!
   */

  /**
   * This method gets the pipeline for the current walog.
   *
   * @return non-null array of DatanodeInfo
   */
  DatanodeInfo[] getPipeLine() {
    if (logFile != null) {
      OutputStream os = logFile.getWrappedStream();
      if (os instanceof DFSOutputStream) {
        return ((DFSOutputStream) os).getPipeline();
      }
    }

    // Don't have a pipeline or can't figure it out.
    return EMPTY_PIPELINE;
  }

}
