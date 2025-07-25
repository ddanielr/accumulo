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
package org.apache.accumulo.tserver.tablet;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.apache.accumulo.core.util.LazySingletons.RANDOM;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.clientImpl.DurabilityImpl;
import org.apache.accumulo.core.conf.AccumuloConfiguration.Deriver;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.constraints.Violations;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.file.FilePrefix;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SourceSwitchingIterator;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.logging.ConditionalLogger.DeduplicatingLogger;
import org.apache.accumulo.core.logging.TabletLogger;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.ConditionalResult;
import org.apache.accumulo.core.metadata.schema.Ample.ConditionalResult.Status;
import org.apache.accumulo.core.metadata.schema.Ample.ConditionalTabletMutator;
import org.apache.accumulo.core.metadata.schema.Ample.ConditionalTabletsMutator;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.scan.ScanDispatch;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.Halt;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.server.compaction.CompactionStats;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.tablets.ConditionCheckerContext.ConditionChecker;
import org.apache.accumulo.server.tablets.TabletNameGenerator;
import org.apache.accumulo.server.tablets.TabletTime;
import org.apache.accumulo.tserver.InMemoryMap;
import org.apache.accumulo.tserver.MinorCompactionReason;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.accumulo.tserver.TabletServerResourceManager.TabletResourceManager;
import org.apache.accumulo.tserver.TabletStatsKeeper;
import org.apache.accumulo.tserver.TabletStatsKeeper.Operation;
import org.apache.accumulo.tserver.TservConstraintEnv;
import org.apache.accumulo.tserver.constraints.ConstraintChecker;
import org.apache.accumulo.tserver.log.DfsLogger;
import org.apache.accumulo.tserver.metrics.TabletServerMinCMetrics;
import org.apache.accumulo.tserver.metrics.TabletServerScanMetrics;
import org.apache.accumulo.tserver.scan.ScanParameters;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

/**
 * Provide access to a single row range in a living TabletServer.
 */
public class Tablet extends TabletBase {
  private static final Logger log = LoggerFactory.getLogger(Tablet.class);
  private static final Logger CLOSING_STUCK_LOGGER =
      new DeduplicatingLogger(log, Duration.ofMinutes(5), 1000);

  private final TabletServer tabletServer;
  private final TabletResourceManager tabletResources;
  private final ScanfileManager scanfileManager;
  private final TabletMemory tabletMemory;

  private final TabletTime tabletTime;

  private final Set<Path> checkedTabletDirs = new ConcurrentSkipListSet<>();

  private final AtomicLong dataSourceDeletions = new AtomicLong(0);

  // This class exists so that a single volatile can reference two variables. Coordinating reads and
  // writes of two separate volatiles that depend on each other is really tricky, putting them under
  // a single volatile removes the tricky part. One key factor to avoiding consistency issues is
  // that instances of this class are immutable, so that should not be changed w/o considering the
  // implications on multithreading.
  private static class LatestMetadata {
    final TabletMetadata tabletMetadata;
    // this exists to detect changes in tabletMetadata
    final long refreshCount;

    private LatestMetadata(TabletMetadata tabletMetadata, long refreshCount) {
      this.tabletMetadata = tabletMetadata;
      this.refreshCount = refreshCount;
    }
  }

  private final AtomicReference<LatestMetadata> latestMetadata;

  @Override
  public long getDataSourceDeletions() {
    return dataSourceDeletions.get();
  }

  private enum CloseState {
    OPEN, REQUESTED, CLOSING, CLOSED, COMPLETE
  }

  private long closeRequestTime = 0;
  private volatile CloseState closeState = CloseState.OPEN;

  private boolean updatingFlushID = false;

  enum CompactionState {
    WAITING_TO_START, IN_PROGRESS
  }

  private volatile CompactionState minorCompactionState = null;

  private final Deriver<ConstraintChecker> constraintChecker;

  private int writesInProgress = 0;

  private final TabletStatsKeeper timer = new TabletStatsKeeper();

  /**
   * Counts are maintained in this object and reported out with the Micrometer metrics via
   * TabletServerMetricsUtil
   */
  private long ingestCount = 0;
  private long ingestBytes = 0;

  /**
   * Rates are calculated here in the Tablet for use in the Monitor but we do not emit them as
   * metrics. Rates can be calculated from the "Count" metrics above by downstream systems.
   */
  private final Rate queryRate = new Rate(0.95);
  private final Rate queryByteRate = new Rate(0.95);
  private final Rate ingestRate = new Rate(0.95);
  private final Rate ingestByteRate = new Rate(0.95);
  private final Rate scannedRate = new Rate(0.95);

  private long lastMinorCompactionFinishTime = 0;

  private volatile long numEntries = 0;
  private volatile long numEntriesInMemory = 0;

  private final int logId;

  private volatile long lastAccessTime = System.nanoTime();

  public int getLogId() {
    return logId;
  }

  public static class LookupResult {
    public final List<Range> unfinishedRanges = new ArrayList<>();
    public long bytesAdded = 0;
    public long dataSize = 0;
    public boolean closed = false;
  }

  ReferencedTabletFile getNextDataFilename(FilePrefix prefix) throws IOException {
    return TabletNameGenerator.getNextDataFilename(prefix, context, extent,
        getMetadata().getDirName(), dir -> checkTabletDir(new Path(dir)));
  }

  private void checkTabletDir(Path path) {
    try {
      if (!checkedTabletDirs.contains(path)) {
        FileStatus[] files = null;
        try {
          files = getTabletServer().getVolumeManager().listStatus(path);
        } catch (FileNotFoundException ex) {
          // ignored
        }

        if (files == null) {
          log.debug("Tablet {} had no dir, creating {}", extent, path);
          getTabletServer().getVolumeManager().mkdirs(path);
        }
        checkedTabletDirs.add(path);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public Tablet(final TabletServer tabletServer, final KeyExtent extent,
      final TabletResourceManager trm, TabletMetadata metadata)
      throws IOException, IllegalArgumentException {

    super(tabletServer, extent);

    this.tabletServer = tabletServer;
    this.tabletResources = trm;
    this.latestMetadata =
        new AtomicReference<>(new LatestMetadata(metadata, RANDOM.get().nextLong()));
    this.tabletTime = TabletTime.getInstance(metadata.getTime());
    this.logId = tabletServer.createLogId();

    constraintChecker = tableConfiguration.newDeriver(ConstraintChecker::new);

    tabletMemory = new TabletMemory(this);

    var logEntries = new ArrayList<>(metadata.getLogs());

    // don't bother examining WALs for recovery if Table is being deleted
    if (!logEntries.isEmpty() && !isBeingDeleted()) {
      TabletLogger.recovering(extent, logEntries);
      final AtomicLong entriesUsedOnTablet = new AtomicLong(0);
      // track max time from walog entries without timestamps
      final AtomicLong maxTime = new AtomicLong(Long.MIN_VALUE);
      final CommitSession commitSession = getTabletMemory().getCommitSession();
      try {
        Set<String> absPaths = new HashSet<>();
        for (StoredTabletFile ref : metadata.getFiles()) {
          absPaths.add(ref.getNormalizedPathStr());
        }

        tabletServer.recover(this.getTabletServer().getVolumeManager(), extent, logEntries,
            absPaths, m -> {
              Collection<ColumnUpdate> muts = m.getUpdates();
              for (ColumnUpdate columnUpdate : muts) {
                if (!columnUpdate.hasTimestamp()) {
                  // if it is not a user set timestamp, it must have been set
                  // by the system
                  maxTime.set(Math.max(maxTime.get(), columnUpdate.getTimestamp()));
                }
              }
              getTabletMemory().mutate(commitSession, Collections.singletonList(m), 1);
              entriesUsedOnTablet.incrementAndGet();
            });

        if (maxTime.get() != Long.MIN_VALUE) {
          tabletTime.updateTimeIfGreater(maxTime.get());
        }
        commitSession.updateMaxCommittedTime(tabletTime.getTime());

        if (entriesUsedOnTablet.get() == 0) {
          log.debug("No replayed mutations applied, removing unused walog entries for {}", extent);

          final Location expectedLocation = Location.future(this.tabletServer.getTabletSession());
          try (ConditionalTabletsMutator mutator =
              getContext().getAmple().conditionallyMutateTablets()) {
            ConditionalTabletMutator mut = mutator.mutateTablet(extent).requireAbsentOperation()
                .requireLocation(expectedLocation);
            logEntries.forEach(mut::deleteWal);
            mut.submit(tabletMetadata -> tabletMetadata.getLogs().isEmpty());

            ConditionalResult res = mutator.process().get(extent);
            if (res.getStatus() == Status.REJECTED) {
              throw new IllegalStateException(
                  "Unable to remove logs in metadata for extent: " + extent);
            }
          }

          // intentionally not rereading metadata here because walogs are only used in the
          // constructor
          logEntries.clear();
        }

      } catch (Exception t) {
        String msg = "Error recovering tablet " + extent + " from log files";
        if (tableConfiguration.getBoolean(Property.TABLE_FAILURES_IGNORE)) {
          log.warn(msg, t);
        } else {
          throw new RuntimeException(msg, t);
        }
      }
      // make some closed references that represent the recovered logs
      currentLogs = new HashSet<>();
      for (LogEntry logEntry : logEntries) {
        currentLogs.add(DfsLogger.fromLogEntry(logEntry));
      }

      rebuildReferencedLogs();

      TabletLogger.recovered(extent, logEntries, entriesUsedOnTablet.get(),
          getTabletMemory().getNumEntries());
    }

    // do this last after tablet is completely setup because it
    // could cause major compaction to start
    scanfileManager = new ScanfileManager(this);

    computeNumEntries();

    getScanfileManager().removeFilesAfterScan(metadata.getScans(),
        Location.future(tabletServer.getTabletSession()));
  }

  public TabletMetadata getMetadata() {
    return latestMetadata.get().tabletMetadata;
  }

  public void checkConditions(ConditionChecker checker, Authorizations authorizations,
      AtomicBoolean iFlag) throws IOException, ReflectiveOperationException {

    ScanParameters scanParams = new ScanParameters(-1, authorizations, Collections.emptySet(), null,
        null, false, null, -1, null);
    scanParams.setScanDispatch(ScanDispatch.builder().build());

    ScanDataSource dataSource = createDataSource(scanParams, false, iFlag);

    boolean sawException = false;
    try {
      SortedKeyValueIterator<Key,Value> iter = new SourceSwitchingIterator(dataSource);
      checker.check(iter);
    } catch (IOException | RuntimeException | ReflectiveOperationException e) {
      sawException = true;
      throw e;
    } finally {
      // code in finally block because always want
      // to return data files, even when exception is thrown
      dataSource.close(sawException);
    }
  }

  DataFileValue minorCompact(InMemoryMap memTable, ReferencedTabletFile tmpDatafile,
      ReferencedTabletFile newDatafile, long queued, CommitSession commitSession, long flushId,
      MinorCompactionReason mincReason) {
    boolean failed = false;
    long start = System.currentTimeMillis();
    timer.incrementStatusMinor();

    long count = 0;

    String oldName = Thread.currentThread().getName();
    try {
      Thread.currentThread().setName("Minor compacting " + this.extent);
      CompactionStats stats;
      Span span = TraceUtil.startSpan(this.getClass(), "minorCompact::write");
      try (Scope scope = span.makeCurrent()) {
        count = memTable.getNumEntries();

        MinorCompactor compactor = new MinorCompactor(tabletServer, this, memTable, tmpDatafile,
            mincReason, tableConfiguration);
        stats = compactor.call();
      } catch (Exception e) {
        TraceUtil.setException(span, e, true);
        throw e;
      } finally {
        span.end();
      }

      Span span2 = TraceUtil.startSpan(this.getClass(), "minorCompact::bringOnline");
      try (Scope scope = span2.makeCurrent()) {
        bringMinorCompactionOnline(tmpDatafile, newDatafile,
            new DataFileValue(stats.getFileSize(), stats.getEntriesWritten()), commitSession,
            flushId, mincReason);
      } catch (Exception e) {
        final ServiceLock tserverLock = tabletServer.getLock();
        if (tserverLock == null || !tserverLock.verifyLockAtSource()) {
          log.error("Minor compaction of {} has failed and TabletServer lock does not exist."
              + " Halting...", getExtent(), e);
          Halt.halt(-1, "TabletServer lock does not exist", e);
        } else {
          TraceUtil.setException(span2, e, true);
          throw e;
        }
      } finally {
        span2.end();
      }

      return new DataFileValue(stats.getFileSize(), stats.getEntriesWritten());
    } catch (Exception | Error e) {
      failed = true;
      throw new RuntimeException("Exception occurred during minor compaction on " + extent, e);
    } finally {
      Thread.currentThread().setName(oldName);
      try {
        getTabletMemory().finalizeMinC();
      } catch (Exception t) {
        log.error("Failed to free tablet memory on {}", extent, t);
      }

      if (!failed) {
        lastMinorCompactionFinishTime = System.currentTimeMillis();
      }
      TabletServerMinCMetrics minCMetrics = getTabletServer().getMinCMetrics();
      minCMetrics.addActive(lastMinorCompactionFinishTime - start);
      timer.updateTime(Operation.MINOR, queued, start, count, failed);
      minCMetrics.addQueued(start - queued);
    }
  }

  private synchronized MinorCompactionTask prepareForMinC(long flushId,
      MinorCompactionReason mincReason) {
    Preconditions.checkState(otherLogs.isEmpty());
    Preconditions.checkState(referencedLogs.equals(currentLogs));
    CommitSession oldCommitSession = getTabletMemory().prepareForMinC();
    otherLogs = currentLogs;
    currentLogs = new HashSet<>();

    return new MinorCompactionTask(this, oldCommitSession, flushId, mincReason);

  }

  public void flush(long tableFlushID) {
    boolean updateMetadata = false;
    boolean initiateMinor = false;

    try {

      synchronized (this) {

        // only want one thing at a time to update flush ID to ensure that metadata table and tablet
        // in memory state are consistent
        if (updatingFlushID) {
          return;
        }

        if (isClosing() || isClosed() || isBeingDeleted()
            || getTabletMemory().memoryReservedForMinC()) {
          return;
        }

        if (getTabletMemory().getMemTable().getNumEntries() == 0) {
          updatingFlushID = true;
          updateMetadata = true;
        } else {
          initiateMinor = true;
        }
      }

      if (updateMetadata) {
        refreshLock.lock();
        try {
          // if multiple threads were allowed to update this outside of a sync block, then it would
          // be a race condition
          var lastTabletMetadata = getMetadata();

          // Check flush id while holding refresh lock to prevent race condition with other threads
          // in tablet reading and writing the tablets metadata.
          if (lastTabletMetadata.getFlushId().orElse(-1) < tableFlushID) {
            try (var tabletsMutator = getContext().getAmple().conditionallyMutateTablets()) {
              var tablet = tabletsMutator.mutateTablet(extent)
                  .requireLocation(Location.current(tabletServer.getTabletSession()))
                  .requireSame(lastTabletMetadata, ColumnType.FLUSH_ID);

              tablet.putFlushId(tableFlushID);
              tablet
                  .submit(tabletMetadata -> tabletMetadata.getFlushId().orElse(-1) == tableFlushID);

              var result = tabletsMutator.process().get(extent);

              if (result.getStatus() != Ample.ConditionalResult.Status.ACCEPTED) {
                throw new IllegalStateException("Failed to update flush id " + extent + " "
                    + tabletServer.getTabletSession() + " " + tableFlushID);
              }
            }

            // It is important the the refresh lock is held for the update above and the refresh
            // below to avoid race conditions.
            refreshMetadata(RefreshPurpose.FLUSH_ID_UPDATE);
          }
        } finally {
          refreshLock.unlock();
        }
      } else if (initiateMinor) {
        initiateMinorCompaction(tableFlushID, MinorCompactionReason.USER);
      }

    } finally {
      if (updateMetadata) {
        synchronized (this) {
          updatingFlushID = false;
          this.notifyAll();
        }
      }
    }

  }

  public boolean initiateMinorCompaction(MinorCompactionReason mincReason) {
    if (isClosed()) {
      return false;
    }
    if (isBeingDeleted()) {
      log.debug("Table {} is being deleted so don't flush {}", extent.tableId(), extent);
      return false;
    }

    // get the flush id before the new memmap is made available for write
    long flushId;
    try {
      flushId = getFlushID();
    } catch (NoNodeException e) {
      log.info("Asked to initiate MinC when there was no flush id {} {}", getExtent(),
          e.getMessage());
      return false;
    }
    return initiateMinorCompaction(flushId, mincReason);
  }

  public boolean minorCompactNow(MinorCompactionReason mincReason) {
    long flushId;
    try {
      flushId = getFlushID();
    } catch (NoNodeException e) {
      log.info("Asked to initiate MinC when there was no flush id {} {}", getExtent(),
          e.getMessage());
      return false;
    }
    MinorCompactionTask mct = createMinorCompactionTask(flushId, mincReason);
    if (mct == null) {
      return false;
    }
    mct.run();
    return true;
  }

  boolean initiateMinorCompaction(long flushId, MinorCompactionReason mincReason) {
    MinorCompactionTask mct = createMinorCompactionTask(flushId, mincReason);
    if (mct == null) {
      return false;
    }
    getTabletResources().executeMinorCompaction(mct);
    return true;
  }

  private MinorCompactionTask createMinorCompactionTask(long flushId,
      MinorCompactionReason mincReason) {
    MinorCompactionTask mct;
    long t1;
    long t2;

    StringBuilder logMessage = null;

    try {
      synchronized (this) {
        t1 = System.currentTimeMillis();

        if (isClosing() || isClosed() || getTabletMemory().memoryReservedForMinC()
            || getTabletMemory().getMemTable().getNumEntries() == 0 || updatingFlushID) {

          logMessage = new StringBuilder();

          logMessage.append(extent);
          logMessage.append(" closeState " + closeState);
          if (getTabletMemory() != null) {
            logMessage.append(" tabletMemory.memoryReservedForMinC() "
                + getTabletMemory().memoryReservedForMinC());
          }
          if (getTabletMemory() != null && getTabletMemory().getMemTable() != null) {
            logMessage.append(" tabletMemory.getMemTable().getNumEntries() "
                + getTabletMemory().getMemTable().getNumEntries());
          }
          logMessage.append(" updatingFlushID " + updatingFlushID);

          return null;
        }

        mct = prepareForMinC(flushId, mincReason);
        t2 = System.currentTimeMillis();
      }
    } finally {
      // log outside of sync block
      if (logMessage != null && log.isDebugEnabled()) {
        log.debug("{}", logMessage);
      }
    }

    log.debug(String.format("MinC initiate lock %.2f secs", (t2 - t1) / 1000.0));
    return mct;
  }

  public long getFlushID() throws NoNodeException {
    try {
      String id = new String(context.getZooSession().asReaderWriter()
          .getData(Constants.ZTABLES + "/" + extent.tableId() + Constants.ZTABLE_FLUSH_ID), UTF_8);
      return Long.parseLong(id);
    } catch (InterruptedException | NumberFormatException e) {
      throw new RuntimeException("Exception on " + extent + " getting flush ID", e);
    } catch (KeeperException ke) {
      if (ke instanceof NoNodeException) {
        throw (NoNodeException) ke;
      } else {
        throw new RuntimeException("Exception on " + extent + " getting flush ID", ke);
      }
    }
  }

  private synchronized CommitSession finishPreparingMutations(long time) {
    if (isClosed() || getTabletMemory() == null) {
      return null;
    }

    CommitSession commitSession = getTabletMemory().getCommitSession();
    incrementWritesInProgress(commitSession);

    commitSession.updateMaxCommittedTime(time);
    return commitSession;
  }

  public PreparedMutations prepareMutationsForCommit(final TservConstraintEnv cenv,
      final List<Mutation> mutations) {
    cenv.setExtent(extent);
    final ConstraintChecker constraints = constraintChecker.derive();

    // Check each mutation for any constraint violations.
    Violations violations = null;
    Set<Mutation> violators = null;
    List<Mutation> nonViolators = null;

    for (Mutation mutation : mutations) {
      Violations mutationViolations = constraints.check(cenv, mutation);
      if (mutationViolations != null) {
        if (violations == null) {
          violations = new Violations();
          violators = new HashSet<>();
        }

        violations.add(mutationViolations);
        violators.add(mutation);
      }
    }

    if (violations == null) {
      // If there are no violations, use the original list for non-violators.
      nonViolators = mutations;
      violators = Collections.emptySet();
      violations = Violations.EMPTY;
    } else if (violators.size() != mutations.size()) {
      // Otherwise, find all non-violators.
      nonViolators = new ArrayList<>((mutations.size() - violators.size()));
      for (Mutation mutation : mutations) {
        if (!violators.contains(mutation)) {
          nonViolators.add(mutation);
        }
      }
    } else {
      // all mutations violated a constraint
      nonViolators = Collections.emptyList();
    }

    // If there are any mutations that do not violate the constraints, attempt to prepare the tablet
    // and retrieve the commit session.
    CommitSession cs = null;
    if (!nonViolators.isEmpty()) {
      long time = tabletTime.setUpdateTimes(nonViolators);
      cs = finishPreparingMutations(time);
      if (cs == null) {
        // tablet is closed
        return new PreparedMutations();
      }
    }

    return new PreparedMutations(cs, nonViolators, violations, violators);
  }

  private synchronized void incrementWritesInProgress(CommitSession cs) {
    incrementWritesInProgress();
    cs.incrementCommitsInProgress();
  }

  private synchronized void incrementWritesInProgress() {
    if (writesInProgress < 0) {
      throw new IllegalStateException("FATAL: Something really bad went wrong. Attempted to "
          + "increment a negative number of writes in progress " + writesInProgress + "on tablet "
          + extent);
    }
    writesInProgress++;
  }

  private synchronized void decrementWritesInProgress(CommitSession cs) {
    decrementWritesInProgress();
    cs.decrementCommitsInProgress();
  }

  private synchronized void decrementWritesInProgress() {
    if (writesInProgress <= 0) {
      throw new IllegalStateException("FATAL: Something really bad went wrong. Attempted to "
          + "decrement the number of writes in progress " + writesInProgress + " to < 0 on tablet "
          + extent);
    }
    writesInProgress--;
    if (writesInProgress == 0) {
      this.notifyAll();
    }
  }

  public synchronized void abortCommit(CommitSession commitSession) {
    if (isCloseComplete() || getTabletMemory() == null) {
      throw new IllegalStateException("Aborting commit when tablet " + extent + " is closed");
    }

    decrementWritesInProgress(commitSession);
  }

  public void commit(CommitSession commitSession, List<Mutation> mutations) {

    int totalCount = 0;
    long totalBytes = 0;

    // write the mutation to the in memory table
    for (Mutation mutation : mutations) {
      totalCount += mutation.size();
      totalBytes += mutation.numBytes();
    }

    try {
      getTabletMemory().mutate(commitSession, mutations, totalCount);
      synchronized (this) {
        getTabletMemory().updateMemoryUsageStats();
        if (isCloseComplete()) {
          throw new IllegalStateException(
              "Tablet " + extent + " closed with outstanding messages to the logger");
        }
        numEntries += totalCount;
        numEntriesInMemory += totalCount;
        ingestCount += totalCount;
        ingestBytes += totalBytes;
      }
    } finally {
      decrementWritesInProgress(commitSession);
    }
  }

  /**
   * Closes the data files associated with a Tablet. If saveState is true, a minor compaction is
   * performed.
   */
  @Override
  public void close(boolean saveState) throws IOException {
    initiateClose(saveState);
    completeClose(saveState);
    log.info("Tablet {} closed.", this.extent);
  }

  void initiateClose(boolean saveState) {
    log.trace("initiateClose(saveState={}) {}", saveState, getExtent());

    synchronized (this) {
      if (closeState == CloseState.OPEN) {
        closeRequestTime = System.nanoTime();
        closeState = CloseState.REQUESTED;
      } else {
        Preconditions.checkState(closeRequestTime != 0);
        long runningTime = Duration.ofNanos(System.nanoTime() - closeRequestTime).toMinutes();
        if (runningTime >= 15) {
          CLOSING_STUCK_LOGGER.info(
              "Tablet {} close requested again, but has been closing for {} minutes", this.extent,
              runningTime);
        }
      }
    }

    MinorCompactionTask mct = null;
    if (saveState) {
      try {
        synchronized (this) {
          // Wait for any running minor compaction before trying to start another. This is done for
          // the case where the current in memory map has a lot of data. So wait for the running
          // minor compaction and then start compacting the current in memory map before closing.
          getTabletMemory().waitForMinC();
        }
        mct = createMinorCompactionTask(getFlushID(), MinorCompactionReason.CLOSE);
      } catch (NoNodeException e) {
        throw new IllegalStateException("Exception on " + extent + " during prep for MinC", e);
      }
    }

    if (mct != null) {
      // Do an initial minor compaction that flushes any data in memory before marking that tablet
      // as closed. Another minor compaction will be done once the tablet is marked as closed. There
      // are two goals for this initial minor compaction.
      //
      // 1. Make the 2nd minor compaction that occurs after closing faster because it has less
      // data. That is important because after the tablet is closed it can not be read or written
      // to, so hopefully the 2nd compaction has little if any data because of this minor compaction
      // that occurred before close.
      //
      // 2. Its possible a minor compaction may hang because of bad config or DFS problems. Taking
      // this action before close can be less disruptive if it does hang. Also in the case where
      // there is a bug that causes minor compaction to fail it will leave the tablet in a bad
      // state. If that happens here before starting to close then it could leave the tablet in a
      // more usable state than a failure that happens after the tablet starts to close.
      //
      // If 'mct' was null it means either a minor compaction was running, there was no data to
      // minor compact, or the flush id was updating. In the case of flush id was updating, ideally
      // this code would wait for flush id updates and then minor compact if needed, but that can
      // not be done without setting the close state to closing to prevent flush id updates from
      // starting. So if there is a flush id update going on it could cause no minor compaction
      // here. There will still be a minor compaction after close.
      //
      // Its important to run the following minor compaction outside of any sync blocks as this
      // could needlessly block scans. The resources needed for the minor compaction have already
      // been reserved in a sync block.
      mct.run();
    }

    synchronized (this) {

      if (saveState) {
        // Wait for any running minc to finish before we start shutting things down in the tablet.
        // It is possible that this function was unable to initiate a minor compaction above and
        // something else did because of race conditions (because everything above happens before
        // marking anything closed so normal actions could still start minor compactions). If
        // something did start lets wait on it before marking things closed.
        getTabletMemory().waitForMinC();
      }

      // This check is intentionally done later in the method because the check and change of the
      // closeState variable need to be atomic, so both are done in the same sync block.
      if (isClosed() || isClosing()) {
        String msg = "Tablet " + getExtent() + " already " + closeState;
        throw new IllegalStateException(msg);
      }

      // enter the closing state, no splits or minor compactions can start
      closeState = CloseState.CLOSING;
      this.notifyAll();
    }

    synchronized (this) {
      Preconditions.checkState(closeState == CloseState.CLOSING);

      while (updatingFlushID) {
        try {
          this.wait(50);
        } catch (InterruptedException e) {
          log.error(e.toString());
        }
      }

      // calling this.wait() releases the lock, ensure things are as expected when the lock is
      // obtained again
      Preconditions.checkState(closeState == CloseState.CLOSING);
    }
  }

  private boolean closeCompleting = false;

  void completeClose(boolean saveState) throws IOException {
    boolean shouldPrepMinC;
    MinorCompactionTask mct = null;

    synchronized (this) {
      if (!isClosing() || isCloseComplete() || closeCompleting) {
        throw new IllegalStateException("Bad close state " + closeState + " on tablet " + extent);
      }

      log.trace("completeClose(saveState={}) {}", saveState, extent);

      // ensure this method is only called once, also guards against multiple
      // threads entering the method at the same time
      closeCompleting = true;
      closeState = CloseState.CLOSED;

      // modify dataSourceDeletions so scans will try to switch data sources and fail because the
      // tablet is closed
      dataSourceDeletions.incrementAndGet();

      for (ScanDataSource activeScan : activeScans) {
        activeScan.interrupt();
      }

      // create a copy so that it can be whittled down as client sessions are disabled
      List<ScanDataSource> runningScans = new ArrayList<>(this.activeScans);

      runningScans.removeIf(scanDataSource -> {
        boolean currentlyUnreserved = disallowNewReservations(scanDataSource.getScanParameters());
        if (currentlyUnreserved) {
          log.debug("Disabled scan session in tablet close {} {}", extent, scanDataSource);
        }
        return currentlyUnreserved;
      });

      long lastLogTime = System.nanoTime();

      // wait for reads and writes to complete
      while (writesInProgress > 0 || !runningScans.isEmpty()) {
        runningScans.removeIf(scanDataSource -> {
          boolean currentlyUnreserved = disallowNewReservations(scanDataSource.getScanParameters());
          if (currentlyUnreserved) {
            log.debug("Disabled scan session in tablet close {} {}", extent, scanDataSource);
          }
          return currentlyUnreserved;
        });

        if (log.isDebugEnabled()
            && System.nanoTime() - lastLogTime > TimeUnit.SECONDS.toNanos(60)) {
          for (ScanDataSource activeScan : runningScans) {
            log.debug("Waiting on scan in completeClose {} {}", extent, activeScan);
          }

          lastLogTime = System.nanoTime();
        }

        try {
          log.debug("Waiting to completeClose for {}. {} writes {} scans", extent, writesInProgress,
              runningScans.size());
          this.wait(50);
        } catch (InterruptedException e) {
          log.error("Interrupted waiting to completeClose for extent {}", extent, e);
        }
      }

      // It is assumed that nothing new would have been added to activeScans since it was copied, so
      // check that assumption. At this point activeScans should be empty or everything in it should
      // be disabled.
      Preconditions.checkState(activeScans.stream()
          .allMatch(scanDataSource -> disallowNewReservations(scanDataSource.getScanParameters())));

      getTabletMemory().waitForMinC();

      shouldPrepMinC = saveState && getTabletMemory().getMemTable().getNumEntries() > 0;
      if (shouldPrepMinC) {
        try {
          mct = prepareForMinC(getFlushID(), MinorCompactionReason.CLOSE);
        } catch (NoNodeException e) {
          throw new RuntimeException("Exception on " + extent + " during prep for MinC", e);
        }
      }
    }

    if (shouldPrepMinC) {
      // must not run while tablet is locked, as this may result in deadlocks
      mct.run();
    }

    synchronized (this) {
      // gave up the lock to allow a minor compaction to run, now that the lock is reacquired
      // validate that nothing unexpectedly changed
      Preconditions.checkState(closeState == CloseState.CLOSED, "closeState:%s extent:%s",
          closeState, extent);
      Preconditions.checkState(writesInProgress == 0, "writesInProgress:%s extent:%s",
          writesInProgress, extent);
      if (saveState) {
        // at this point all tablet data is flushed, so do a consistency check
        RuntimeException err = null;
        for (int i = 0; i < 5; i++) {
          try {
            closeConsistencyCheck();
            err = null;
          } catch (RuntimeException t) {
            err = t;
            log.error("Consistency check fails, retrying", t);
            sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
          }
        }
        if (err != null) {
          log.error("Tablet closed consistency check has failed for {} giving up and closing",
              this.extent);
        }
      }

      try {
        getTabletMemory().getMemTable().delete(0);
      } catch (Exception t) {
        log.error("Failed to delete mem table : " + t.getMessage() + " for tablet " + extent, t);
      }

      getTabletMemory().close();

      // close data files
      getTabletResources().close();

      closeState = CloseState.COMPLETE;
    }
  }

  private void closeConsistencyCheck() {

    long num = tabletMemory.getMemTable().getNumEntries();
    if (num != 0) {
      String msg = "Closed tablet " + extent + " has " + num + " entries in memory";
      log.error(msg);
      throw new RuntimeException(msg);
    }

    if (tabletMemory.memoryReservedForMinC()) {
      String msg = "Closed tablet " + extent + " has minor compacting memory";
      log.error(msg);
      throw new RuntimeException(msg);
    }

    try {
      var tabletMeta = context.getAmple().readTablet(extent, ColumnType.FILES, ColumnType.LOGS,
          ColumnType.ECOMP, ColumnType.PREV_ROW, ColumnType.FLUSH_ID);

      if (tabletMeta == null) {
        String msg = "Closed tablet " + extent + " not found in metadata";
        log.error(msg);
        throw new RuntimeException(msg);
      }

      if (!tabletMeta.getLogs().isEmpty()) {
        String msg = "Closed tablet " + extent + " has walog entries in "
            + SystemTables.METADATA.tableName() + " " + tabletMeta.getLogs();
        log.error(msg);
        throw new RuntimeException(msg);
      }
    } catch (Exception e) {
      String msg = "Failed to do close consistency check for tablet " + extent;
      log.error(msg, e);
      throw new RuntimeException(msg, e);

    }

    if (!otherLogs.isEmpty() || !currentLogs.isEmpty() || !referencedLogs.isEmpty()) {
      String msg = "Closed tablet " + extent + " has walog entries in memory currentLogs = "
          + currentLogs + "  otherLogs = " + otherLogs + " referencedLogs = " + referencedLogs;
      log.error(msg);
      throw new RuntimeException(msg);
    }
  }

  synchronized void computeNumEntries() {
    Collection<DataFileValue> vals = getDatafiles().values();

    long numEntries = 0;

    for (DataFileValue tableValue : vals) {
      numEntries += tableValue.getNumEntries();
    }

    this.numEntriesInMemory = getTabletMemory().getNumEntries();
    numEntries += getTabletMemory().getNumEntries();

    this.numEntries = numEntries;
  }

  public long getNumEntries() {
    return numEntries;
  }

  public long getNumEntriesInMemory() {
    return numEntriesInMemory;
  }

  // Do not synchronize this method, it is called frequently by compactions
  public boolean isClosing() {
    return closeState == CloseState.CLOSING;
  }

  @Override
  public boolean isClosed() {
    // Assign to a local var to avoid race conditions since closeState is volatile and two
    // comparisons are done.
    CloseState localCS = closeState;
    return localCS == CloseState.CLOSED || localCS == CloseState.COMPLETE;
  }

  public boolean isBeingDeleted() {
    return context.getTableManager().getTableState(extent.tableId()) == TableState.DELETING;
  }

  public boolean isCloseComplete() {
    return closeState == CloseState.COMPLETE;
  }

  public boolean isMinorCompactionQueued() {
    return minorCompactionState == CompactionState.WAITING_TO_START;
  }

  public boolean isMinorCompactionRunning() {
    return minorCompactionState == CompactionState.IN_PROGRESS;
  }

  @Override
  public Map<StoredTabletFile,DataFileValue> getDatafiles() {
    return getMetadata().getFilesMap();
  }

  @Override
  public void addToYieldMetric(int i) {
    getTabletServer().getScanMetrics().addYield(i);
  }

  public double queryRate() {
    return queryRate.rate();
  }

  public double queryByteRate() {
    return queryByteRate.rate();
  }

  public double ingestRate() {
    return ingestRate.rate();
  }

  public double ingestByteRate() {
    return ingestByteRate.rate();
  }

  public double scanRate() {
    return scannedRate.rate();
  }

  public long totalQueriesResults() {
    return this.queryResultCount.get();
  }

  public long totalIngest() {
    return this.ingestCount;
  }

  public long totalIngestBytes() {
    return this.ingestBytes;
  }

  public synchronized void updateRates(long now) {
    queryRate.update(now, this.queryResultCount.get());
    queryByteRate.update(now, this.queryResultBytes.get());
    ingestRate.update(now, ingestCount);
    ingestByteRate.update(now, ingestBytes);
    scannedRate.update(now, this.scannedCount.get());
  }

  private Set<DfsLogger> currentLogs = new HashSet<>();
  private Set<DfsLogger> otherLogs = Collections.emptySet();

  // An immutable copy of currentLogs + otherLogs. This exists so that removeInUseLogs() does not
  // have to get the tablet lock. See #558
  private volatile Set<DfsLogger> referencedLogs = Collections.emptySet();

  private synchronized void rebuildReferencedLogs() {
    /*
     * Each tablet has the following sets of WALogs. While a WALog exists in one set, garbage
     * collection must be avoided.
     *
     * 1. WALogs for the active in memory map
     *
     * 2. WAlogs for the minor compacting in memory map
     *
     * 3. WAlogs for a newly minor compacted file that is being added to the metadata table.
     *
     * Set 1 is currentLogs. Set 2 is otherLogs. Set 3 only exist in referenced logs as a side
     * effect of not calling this method in beginClearingUnusedLogs() when otherLogs is cleared.
     *
     * Ensuring referencedLogs accurately tracks these sets ensures in use walogs are not GCed.
     */

    var prev = referencedLogs;

    referencedLogs = Stream.concat(currentLogs.stream(), otherLogs.stream())
        .collect(Collectors.toUnmodifiableSet());

    if (TabletLogger.isWalRefLoggingEnabled() && !prev.equals(referencedLogs)) {
      TabletLogger.walRefsChanged(extent,
          referencedLogs.stream().map(DfsLogger::getPath).map(Path::getName).collect(toList()));
    }

  }

  public void removeInUseLogs(Set<DfsLogger> candidates) {
    candidates.removeAll(referencedLogs);
  }

  public void checkIfMinorCompactionNeededForLogs(List<DfsLogger> closedLogs, int maxLogs) {

    String reason = null;
    synchronized (this) {
      if (currentLogs.size() >= maxLogs) {
        reason = "referenced " + currentLogs.size() + " write ahead logs";
      } else if (maxLogs < closedLogs.size()) {
        // If many tablets reference a single WAL, but each tablet references a different WAL then
        // this could result in the tablet server referencing many WALs. For recovery that would
        // mean each tablet had to process lots of WAL. This check looks for a single use of an
        // older WAL and compacts if one is found. The following check assumes the most recent WALs
        // are at the end of the list and ignores these.
        List<DfsLogger> oldClosed = closedLogs.subList(0, closedLogs.size() - maxLogs);
        for (DfsLogger closedLog : oldClosed) {
          if (currentLogs.contains(closedLog)) {
            reason = "referenced at least one old write ahead log " + closedLog.getLogEntry();
            break;
          }
        }
      }
    }

    if (reason != null) {
      // initiate and log outside of tablet lock
      log.debug("Initiating minor compaction for {} because {}", getExtent(), reason);
      initiateMinorCompaction(MinorCompactionReason.SYSTEM);
    }
  }

  Set<LogEntry> beginClearingUnusedLogs() {
    Preconditions.checkState(logLock.isHeldByCurrentThread());
    Set<LogEntry> unusedLogs = new HashSet<>();

    ArrayList<LogEntry> otherLogsCopy = new ArrayList<>();
    ArrayList<LogEntry> currentLogsCopy = new ArrayList<>();

    synchronized (this) {
      if (removingLogs) {
        throw new IllegalStateException(
            "Attempted to clear logs when removal of logs in progress on " + extent);
      }

      for (DfsLogger logger : otherLogs) {
        otherLogsCopy.add(logger.getLogEntry());
        unusedLogs.add(logger.getLogEntry());
      }

      for (DfsLogger logger : currentLogs) {
        currentLogsCopy.add(logger.getLogEntry());
        unusedLogs.remove(logger.getLogEntry());
      }

      if (!unusedLogs.isEmpty()) {
        removingLogs = true;
      }
    }

    // do debug logging outside tablet lock
    for (LogEntry logEntry : otherLogsCopy) {
      log.trace("Logs for memory compacted: {} {}", getExtent(), logEntry);
    }

    for (LogEntry logEntry : currentLogsCopy) {
      log.trace("Logs for current memory: {} {}", getExtent(), logEntry);
    }

    for (LogEntry logEntry : unusedLogs) {
      log.trace("Logs to be destroyed: {} {}", getExtent(), logEntry);
    }

    return unusedLogs;
  }

  synchronized void finishClearingUnusedLogs() {
    Preconditions.checkState(logLock.isHeldByCurrentThread());
    removingLogs = false;
    otherLogs = Collections.emptySet();
    rebuildReferencedLogs();
  }

  private boolean removingLogs = false;

  // this lock is basically used to synchronize writing of log info to metadata
  // care should be taken when using this lock. Lock order should be:
  // refreshLock -> logLock -> tablet to prevent deadlock
  private final ReentrantLock logLock = new ReentrantLock();

  // don't release the lock if this method returns true for success; instead, the caller should
  // clean up by calling finishUpdatingLogsUsed()
  @SuppressFBWarnings(value = "UL_UNRELEASED_LOCK",
      justification = "lock is released by caller calling finishedUpdatingLogsUsed method")
  public boolean beginUpdatingLogsUsed(InMemoryMap memTable, DfsLogger more, boolean mincFinish) {

    boolean releaseLock = true;

    logLock.lock();

    try {
      synchronized (this) {

        if (isCloseComplete()) {
          throw new IllegalStateException("Can not update logs of closed tablet " + extent);
        }

        boolean addToOther;

        if (memTable == getTabletMemory().getMinCMemTable()) {
          addToOther = true;
        } else if (memTable == getTabletMemory().getMemTable()) {
          addToOther = false;
        } else {
          throw new IllegalArgumentException("Passed in memtable that is not in use for " + extent);
        }

        if (mincFinish) {
          if (addToOther) {
            throw new IllegalStateException("Adding to other logs for mincFinish on " + extent);
          }
          if (!otherLogs.isEmpty()) {
            throw new IllegalStateException("Expect other logs to be 0 when minC finish, but its "
                + otherLogs + " for " + extent);
          }

          // when writing a minc finish event, there is no need to add the log to metadata
          // if nothing has been logged for the tablet since the minor compaction started
          if (currentLogs.isEmpty()) {
            return !releaseLock;
          }
        }

        boolean added;
        boolean contained;
        if (addToOther) {
          added = otherLogs.add(more);
          contained = currentLogs.contains(more);
        } else {
          added = currentLogs.add(more);
          contained = otherLogs.contains(more);
        }

        if (added) {
          rebuildReferencedLogs();
        }

        if (added && !contained) {
          releaseLock = false;
        }

        return !releaseLock;
      }
    } finally {
      if (releaseLock) {
        logLock.unlock();
      }
    }
  }

  public void finishUpdatingLogsUsed() {
    logLock.unlock();
  }

  public Durability getDurability() {
    return DurabilityImpl.fromString(getTableConfiguration().get(Property.TABLE_DURABILITY));
  }

  public void updateMemoryUsageStats(long size, long mincSize) {
    getTabletResources().updateMemoryUsageStats(this, size, mincSize);
  }

  TabletServer getTabletServer() {
    return tabletServer;
  }

  /**
   * Update tablet file data from flush. Returns a StoredTabletFile if there are data entries.
   */
  private Optional<StoredTabletFile> updateTabletDataFile(long maxCommittedTime,
      ReferencedTabletFile newDatafile, DataFileValue dfv, Set<LogEntry> unusedWalLogs,
      long flushId, MinorCompactionReason mincReason) {

    Preconditions.checkState(refreshLock.isHeldByCurrentThread());

    // Read these once in case of buggy race conditions will get consistent logging. If all other
    // code is locking properly these should not change during this method.
    var lastTabletMetadata = getMetadata();

    return updateTabletDataFile(getContext().getAmple(), maxCommittedTime, newDatafile, dfv,
        unusedWalLogs, flushId, mincReason, tabletServer.getTabletSession(), extent,
        lastTabletMetadata, tabletTime, RANDOM.get().nextLong());
  }

  @VisibleForTesting
  public static Optional<StoredTabletFile> updateTabletDataFile(Ample ample, long maxCommittedTime,
      ReferencedTabletFile newDatafile, DataFileValue dfv, Set<LogEntry> unusedWalLogs,
      long flushId, MinorCompactionReason mincReason, TServerInstance tserverInstance,
      KeyExtent extent, TabletMetadata lastTabletMetadata, TabletTime tabletTime, long flushNonce) {
    while (true) {
      try (var tabletsMutator = ample.conditionallyMutateTablets()) {

        var expectedLocation = mincReason == MinorCompactionReason.RECOVERY
            ? Location.future(tserverInstance) : Location.current(tserverInstance);

        var tablet = tabletsMutator.mutateTablet(extent).requireLocation(expectedLocation);

        Optional<StoredTabletFile> newFile = Optional.empty();

        // if entries are present, write to path to metadata table
        if (dfv.getNumEntries() > 0) {
          tablet.putFile(newDatafile, dfv);
          newFile = Optional.of(newDatafile.insert());
        }

        boolean setTime = false;
        // bulk imports can also update time in the metadata table, so only update if we are moving
        // time forward
        if (maxCommittedTime > lastTabletMetadata.getTime().getTime()) {
          tablet.requireSame(lastTabletMetadata, ColumnType.TIME);
          var newTime = tabletTime.getMetadataTime(maxCommittedTime);
          tablet.putTime(newTime);
          setTime = true;
        }

        tablet.putFlushId(flushId);

        tablet.putFlushNonce(flushNonce);

        unusedWalLogs.forEach(tablet::deleteWal);

        // When trying to determine if write was successful, check if the flush nonce was updated.
        // Can not check if the new file exists because of two reasons. First, it could be compacted
        // away between the write and check. Second, some flushes do not produce a file.
        tablet.submit(tabletMetadata -> {
          var persistedNonce = tabletMetadata.getFlushNonce();
          if (persistedNonce.isPresent()) {
            return persistedNonce.getAsLong() == flushNonce;
          }
          return false;
        });

        var result = tabletsMutator.process().get(extent);
        if (result.getStatus() == Status.ACCEPTED) {
          return newFile;
        } else {
          var updatedTableMetadata = result.readMetadata();
          if (setTime && expectedLocation.equals(updatedTableMetadata.getLocation())
              && !lastTabletMetadata.getTime().equals(updatedTableMetadata.getTime())) {
            // The update failed because the time changed, so lets try again.
            log.debug("Failed to add {} to {} because time changed {}!={}, will retry", newFile,
                extent, lastTabletMetadata.getTime(), updatedTableMetadata.getTime());
            lastTabletMetadata = updatedTableMetadata;
            UtilWaitThread.sleep(1000);
          } else {
            log.error("Metadata for failed tablet file update : {}", updatedTableMetadata);
            // Include the things that could have caused the write to fail.
            throw new IllegalStateException(
                "Unable to add file to tablet.  " + extent + " " + expectedLocation);
          }
        }
      }
    }
  }

  @Override
  TabletResourceManager getTabletResources() {
    return tabletResources;
  }

  @Override
  public TabletServerScanMetrics getScanMetrics() {
    return getTabletServer().getScanMetrics();
  }

  ScanfileManager getScanfileManager() {
    return scanfileManager;
  }

  @Override
  public Pair<Long,Map<StoredTabletFile,DataFileValue>> reserveFilesForScan() {
    return getScanfileManager().reserveFilesForScan();
  }

  @Override
  public void returnFilesForScan(long scanId) {
    getScanfileManager().returnFilesForScan(scanId);
  }

  public void removeBatchedScanRefs() {
    synchronized (this) {
      if (isClosed() || isClosing()) {
        return;
      }
      // return early if there are no scan files to remove
      if (!getScanfileManager().canScanRefsBeRemoved()) {
        return;
      }
      incrementWritesInProgress();
    }
    try {
      getScanfileManager().removeBatchedScanRefs();
    } finally {
      decrementWritesInProgress();
    }
  }

  TabletMemory getTabletMemory() {
    return tabletMemory;
  }

  @Override
  public List<InMemoryMap.MemoryIterator> getMemIterators(SamplerConfigurationImpl samplerConfig) {
    return getTabletMemory().getIterators(samplerConfig);
  }

  @Override
  public void returnMemIterators(List<InMemoryMap.MemoryIterator> iters) {
    getTabletMemory().returnIterators(iters);
  }

  public void minorCompactionWaitingToStart() {
    minorCompactionState = CompactionState.WAITING_TO_START;
  }

  public void minorCompactionStarted() {
    minorCompactionState = CompactionState.IN_PROGRESS;
  }

  public void minorCompactionComplete() {
    minorCompactionState = null;
  }

  public TabletStats getTabletStats() {
    return timer.getTabletStats();
  }

  public boolean isOnDemand() {
    // TODO a change in the tablet availability could refresh online tablets
    return getMetadata().getTabletAvailability() == TabletAvailability.ONDEMAND;
  }

  // The purpose of this lock is to prevent race conditions between concurrent refresh RPC calls and
  // between minor compactions and refresh calls.
  // care should be taken when using this lock. Lock order should be:
  // refreshLock -> logLock -> tablet to prevent deadlock
  private final ReentrantLock refreshLock = new ReentrantLock();

  void bringMinorCompactionOnline(ReferencedTabletFile tmpDatafile,
      ReferencedTabletFile newDatafile, DataFileValue dfv, CommitSession commitSession,
      long flushId, MinorCompactionReason mincReason) {
    Optional<StoredTabletFile> newFile;
    // rename before putting in metadata table, so files in metadata table should
    // always exist
    boolean attemptedRename = false;
    VolumeManager vm = getTabletServer().getContext().getVolumeManager();
    do {
      try {
        if (dfv.getNumEntries() == 0) {
          log.debug("No data entries so delete temporary file {}", tmpDatafile);
          vm.deleteRecursively(tmpDatafile.getPath());
        } else {
          if (!attemptedRename && vm.exists(newDatafile.getPath())) {
            log.warn("Target data file already exist {}", newDatafile);
            throw new RuntimeException("File unexpectedly exists " + newDatafile.getPath());
          }
          // the following checks for spurious rename failures that succeeded but gave an IoE
          if (attemptedRename && vm.exists(newDatafile.getPath())
              && !vm.exists(tmpDatafile.getPath())) {
            // seems like previous rename succeeded, so break
            break;
          }
          attemptedRename = true;
          ScanfileManager.rename(vm, tmpDatafile.getPath(), newDatafile.getPath());
        }
        break;
      } catch (IOException ioe) {
        log.warn("Tablet " + getExtent() + " failed to rename " + newDatafile
            + " after MinC, will retry in 60 secs...", ioe);
        sleepUninterruptibly(1, TimeUnit.MINUTES);
      }
    } while (true);

    // The refresh lock must be held for the metadata write that adds the new file to the tablet.
    // This prevents a concurrent refresh operation from pulling in the new tablet file before the
    // in memory map reference related to the file is deactivated. Scans should use one of the in
    // memory map or the new file, never both.
    Preconditions.checkState(!logLock.isHeldByCurrentThread());
    refreshLock.lock();
    try {
      // Can not hold tablet lock while acquiring the log lock.
      logLock.lock();
      // do not place any code here between lock and try
      try {
        // The following call pairs with tablet.finishClearingUnusedLogs() later in this block. If
        // moving where the following method is called, examine it and finishClearingUnusedLogs()
        // before moving.
        Set<LogEntry> unusedWalLogs = beginClearingUnusedLogs();
        // the order of writing to metadata and walog is important in the face of machine/process
        // failures need to write to metadata before writing to walog, when things are done in the
        // reverse order data could be lost... the minor compaction start event should be written
        // before the following metadata write is made

        newFile = updateTabletDataFile(commitSession.getMaxCommittedTime(), newDatafile, dfv,
            unusedWalLogs, flushId, mincReason);

        finishClearingUnusedLogs();
      } finally {
        logLock.unlock();
      }

      // Without the refresh lock, if a refresh happened here it could make the new file written to
      // the metadata table above available for scans while the in memory map from which the file
      // was produced is still available for scans

      do {
        try {
          // the purpose of making this update use the new commit session, instead of the old one
          // passed in, is because the new one will reference the logs used by current memory...
          getTabletServer().minorCompactionFinished(getTabletMemory().getCommitSession(),
              commitSession.getWALogSeq() + 2);
          break;
        } catch (IOException e) {
          log.error("Failed to write to write-ahead log " + e.getMessage() + " will retry", e);
          sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
      } while (true);

      refreshMetadata(RefreshPurpose.MINC_COMPLETION);
    } finally {
      refreshLock.unlock();
    }
    TabletLogger.flushed(getExtent(), newFile);

    long splitSize = getTableConfiguration().getAsBytes(Property.TABLE_SPLIT_THRESHOLD);
    if (dfv.getSize() > splitSize) {
      log.debug(String.format("Minor Compaction wrote out file larger than split threshold."
          + " split threshold = %,d  file size = %,d", splitSize, dfv.getSize()));
    }
  }

  public enum RefreshPurpose {
    MINC_COMPLETION, REFRESH_RPC, FLUSH_ID_UPDATE, LOAD
  }

  public class RefreshSession {

    private final long observedRefreshCount;

    private RefreshSession(long observedRefreshCount) {
      this.observedRefreshCount = observedRefreshCount;
    }

    /**
     * Refresh tablet metadata using metadata that was read separately.
     *
     * @param tabletMetadata this tablet metadata must have been read after calling
     *        {@link Tablet#startRefresh()}
     */
    public boolean refreshMetadata(RefreshPurpose refreshPurpose, TabletMetadata tabletMetadata) {
      return Tablet.this.refreshMetadata(refreshPurpose, observedRefreshCount, tabletMetadata);
    }
  }

  /**
   * A refresh session allows code outside of this class to safely read tablet metadata and pass it
   * back. This is useful for the case where many tablets need to be refreshed and we want to batch
   * reading their metadata. Creating a refresh session will not block. A refresh session is able to
   * detect changes in tablet metadata that happen during its existence and reread tablet metadata
   * if necessary.
   */
  public RefreshSession startRefresh() {
    return new RefreshSession(latestMetadata.get().refreshCount);
  }

  private boolean refreshMetadata(RefreshPurpose refreshPurpose, Long observedRefreshCount,
      TabletMetadata tabletMetadata) {

    refreshLock.lock();
    try {
      var prevMetadata = latestMetadata.get();
      // if the tablet metadata passed in is stale, then reread it
      if (observedRefreshCount == null || !observedRefreshCount.equals(prevMetadata.refreshCount)) {
        if (observedRefreshCount != null) {
          log.debug(
              "Metadata read outside of refresh lock is no longer valid, rereading metadata. {} {} {}",
              extent, observedRefreshCount, prevMetadata.refreshCount);
        }
        // do not want to hold tablet lock while doing metadata read as this could negatively impact
        // scans
        tabletMetadata = getContext().getAmple().readTablet(getExtent());
        if (tabletMetadata == null) {
          log.debug(
              "Unable to refresh tablet {} for {} because it no longer exists in metadata table",
              extent, refreshPurpose);
          return false;
        }
      } else {
        // when observedRefreshCount is not null, tabletMetadata must not be null
        Preconditions.checkArgument(tabletMetadata != null);
      }

      if (tabletMetadata.getLocation() == null || !tabletServer.getTabletSession()
          .equals(tabletMetadata.getLocation().getServerInstance())) {
        log.debug("Unable to refresh tablet {} for {} because it has a different location {}",
            extent, refreshPurpose, tabletMetadata.getLocation());
        return false;
      }

      synchronized (this) {
        if (isCloseComplete()) {
          log.debug("Unable to refresh tablet {} for {} because the tablet is closed", extent,
              refreshPurpose);
          return false;
        }

        // Its expected that what is persisted should be less than equal to the time that tablet has
        // in memory.
        Preconditions.checkState(tabletMetadata.getTime().getTime() <= tabletTime.getTime(),
            "Time in metadata is ahead of tablet %s memory:%s metadata:%s", extent,
            tabletTime.getTime(), tabletMetadata.getTime());

        // must update latestMetadata before computeNumEntries() is called
        Preconditions.checkState(
            latestMetadata.compareAndSet(prevMetadata,
                new LatestMetadata(tabletMetadata, prevMetadata.refreshCount + 1)),
            "A concurrency bug exists in the code, something is setting latestMetadata without holding the refreshLock.");

        if (refreshPurpose == RefreshPurpose.MINC_COMPLETION) {
          // Atomically replace the in memory map with the new file. Before this synch block a scan
          // starting would see the in memory map. After this synch block it should see the file in
          // the tabletMetadata. Scans sync on the tablet also, so they can not be in this code
          // block at the same time.

          tabletMemory.finishedMinC();

          // the files and in memory map changed, incrementing this will cause scans to switch data
          // sources
          dataSourceDeletions.incrementAndGet();

          // important to call this after updating latestMetadata and tabletMemory
          computeNumEntries();
        } else if (!prevMetadata.tabletMetadata.getFilesMap().equals(getMetadata().getFilesMap())) {

          // the files changed, incrementing this will cause scans to switch data sources
          dataSourceDeletions.incrementAndGet();

          // important to call this after updating latestMetadata
          computeNumEntries();
        }
      }

      if (log.isDebugEnabled()
          && !prevMetadata.tabletMetadata.getFiles().equals(getMetadata().getFiles())) {
        SetView<StoredTabletFile> removed =
            Sets.difference(prevMetadata.tabletMetadata.getFiles(), getMetadata().getFiles());
        SetView<StoredTabletFile> added =
            Sets.difference(getMetadata().getFiles(), prevMetadata.tabletMetadata.getFiles());
        log.debug("Tablet {} was refreshed because {}. Files removed: [{}] Files added: [{}]",
            this.getExtent(), refreshPurpose,
            removed.stream().map(StoredTabletFile::getFileName).collect(Collectors.joining(",")),
            added.stream().map(StoredTabletFile::getFileName).collect(Collectors.joining(",")));
      }
    } finally {
      refreshLock.unlock();
    }

    if (refreshPurpose == RefreshPurpose.REFRESH_RPC) {
      scanfileManager.removeFilesAfterScan(getMetadata().getScans(),
          Location.current(tabletServer.getTabletSession()));
    }

    return true;
  }

  public void refreshMetadata(RefreshPurpose refreshPurpose) {
    Preconditions.checkState(refreshMetadata(refreshPurpose, null, null), "Failed to refresh %s",
        extent);
  }

  public long getLastAccessTime() {
    return lastAccessTime;
  }

  public void setLastAccessTime() {
    this.lastAccessTime = System.nanoTime();
  }

  public synchronized boolean isInUse() {
    // We can't use the lastAccessTime to determine if a Tablet is in use
    // because it is only set when TabletServer.getOnlineTablet is called
    // **and** that method is not called in every case where the Tablet
    // is used.
    return !activeScans.isEmpty() || writesInProgress > 0;
  }

  public synchronized OptionalLong allocateTimestamp() {
    if (isClosing() || isClosed()) {
      return OptionalLong.empty();
    }
    var time = tabletTime.getAndUpdateTime();
    getTabletMemory().getCommitSession().updateMaxCommittedTime(time);
    return OptionalLong.of(time);
  }
}
