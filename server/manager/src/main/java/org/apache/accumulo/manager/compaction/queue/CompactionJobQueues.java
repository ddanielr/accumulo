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
package org.apache.accumulo.manager.compaction.queue;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap.KeySetView;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactorGroupId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

public class CompactionJobQueues {

  private static final Logger log = LoggerFactory.getLogger(CompactionJobQueues.class);

  // The code in this class specifically depends on the behavior of ConcurrentHashMap, behavior that
  // other ConcurrentMap implementations may not have. The behavior it depended on is that the
  // compute functions for a key are atomic. This is documented on its javadoc and in the impl it
  // can be observed that scoped locks are acquired. Other concurrent map impls may run the compute
  // lambdas concurrently for a given key, which may still be correct but is more difficult to
  // analyze.
  private final ConcurrentHashMap<CompactorGroupId,CompactionJobPriorityQueue> priorityQueues =
      new ConcurrentHashMap<>();
  private final Map<CompactorGroupId,Integer> maxJobs;
  private Integer defaultMaxJobs;

  private final Map<DataLevel,AtomicLong> currentGenerations;

  public CompactionJobQueues(Map<CompactorGroupId,Integer> maxJobs, Integer defaultMaxJobs) {
    this.maxJobs = maxJobs;
    this.defaultMaxJobs = defaultMaxJobs;

    Map<DataLevel,AtomicLong> cg = new EnumMap<>(DataLevel.class);
    for (var level : DataLevel.values()) {
      cg.put(level, new AtomicLong());
    }
    currentGenerations = Collections.unmodifiableMap(cg);

  }

  public void beginFullScan(DataLevel level) {
    currentGenerations.get(level).incrementAndGet();
  }

  /**
   * The purpose of this method is to remove any tablets that were added before beginFullScan() was
   * called. The purpose of this is to handle tablets that were queued for compaction for a while
   * and because of some change no longer need to compact. If a full scan of the metadata table does
   * not find any new work for tablet, then any previously queued work for that tablet should be
   * discarded.
   *
   * @param level full metadata scans are done independently per DataLevel, so the tracking what
   *        needs to be removed must be done per DataLevel
   */
  public void endFullScan(DataLevel level) {
    priorityQueues.values()
        .forEach(pq -> pq.removeOlderGenerations(level, currentGenerations.get(level).get()));
  }

  public void setMaxJobs(Map<CompactorGroupId,Integer> queueSizes) {
    Set<CompactorGroupId> currentQueues = new HashSet<>(getQueueIds());
    log.debug("update - current compaction queues {}", currentQueues);

    SetView<CompactorGroupId> oldQueues = Sets.difference(queueSizes.keySet(), currentQueues);
    oldQueues.forEach(q -> {
      log.debug("update - removing compaction queue: {}", q);
      maxJobs.remove(q);
      priorityQueues.remove(q);
    });

    // Add new queue sizes or update existing sizes and remove the old queue.

    for (Map.Entry<CompactorGroupId,Integer> entry : queueSizes.entrySet()) {
      if (!maxJobs.containsKey(entry.getKey())) {
        maxJobs.put(entry.getKey(), entry.getValue());
      } else if (!maxJobs.get(entry.getKey()).equals(entry.getValue())) {
        log.debug("update - removing compaction queue: {} due to size change", entry.getKey());
        maxJobs.put(entry.getKey(), entry.getValue());
        priorityQueues.remove(entry.getKey());
      }
    }
  }

  public void setDefaultMaxJobs(Integer defaultMaxJobs) {
    this.defaultMaxJobs = defaultMaxJobs;
  }

  public void add(TabletMetadata tabletMetadata, Collection<CompactionJob> jobs) {
    if (jobs.size() == 1) {
      var executorId = jobs.iterator().next().getGroup();
      add(tabletMetadata, executorId, jobs);
    } else {
      jobs.stream().collect(Collectors.groupingBy(CompactionJob::getGroup))
          .forEach(((groupId, compactionJobs) -> add(tabletMetadata, groupId, compactionJobs)));
    }
  }

  public KeySetView<CompactorGroupId,CompactionJobPriorityQueue> getQueueIds() {
    return priorityQueues.keySet();
  }

  public CompactionJobPriorityQueue getQueue(CompactorGroupId groupId) {
    return priorityQueues.get(groupId);
  }

  public long getQueueCount() {
    return priorityQueues.mappingCount();
  }

  public long getQueuedJobCount() {
    long count = 0;
    for (CompactionJobPriorityQueue queue : priorityQueues.values()) {
      count += queue.getQueuedJobs();
    }
    return count;
  }

  public static class MetaJob {
    private final CompactionJob job;

    // the metadata from which the compaction job was derived
    private final TabletMetadata tabletMetadata;

    public MetaJob(CompactionJob job, TabletMetadata tabletMetadata) {
      this.job = job;
      this.tabletMetadata = tabletMetadata;
    }

    public CompactionJob getJob() {
      return job;
    }

    public TabletMetadata getTabletMetadata() {
      return tabletMetadata;
    }
  }

  public MetaJob poll(CompactorGroupId groupId) {
    var prioQ = priorityQueues.get(groupId);
    if (prioQ == null) {
      return null;
    }
    MetaJob mj = prioQ.poll();

    if (mj == null) {
      priorityQueues.computeIfPresent(groupId, (eid, pq) -> {
        if (pq.closeIfEmpty()) {
          return null;
        } else {
          return pq;
        }
      });
    }
    return mj;
  }

  private void add(TabletMetadata tabletMetadata, CompactorGroupId groupId,
      Collection<CompactionJob> jobs) {

    if (log.isTraceEnabled()) {
      log.trace("Adding jobs to queue {} {} {}", groupId, tabletMetadata.getExtent(),
          jobs.stream().map(job -> "#files:" + job.getFiles().size() + ",prio:" + job.getPriority()
              + ",kind:" + job.getKind()).collect(Collectors.toList()));
    }
    var queueLength = maxJobs.getOrDefault(groupId, defaultMaxJobs);

    // Add null check and drop if the compaction queue isn't "known" in our config
    // log.debug statement

    var pq = priorityQueues.computeIfAbsent(groupId,
        gid -> new CompactionJobPriorityQueue(gid, queueLength));
    while (pq.add(tabletMetadata, jobs,
        currentGenerations.get(DataLevel.of(tabletMetadata.getTableId())).get()) < 0) {
      // When entering this loop its expected the queue is closed
      Preconditions.checkState(pq.isClosed());
      // This loop handles race condition where poll() closes empty priority queues. The queue could
      // be closed after its obtained from the map and before add is called.
      pq = priorityQueues.computeIfAbsent(groupId,
          gid -> new CompactionJobPriorityQueue(gid, queueLength));
    }
  }
}
