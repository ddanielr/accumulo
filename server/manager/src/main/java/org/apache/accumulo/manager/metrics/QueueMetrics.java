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
package org.apache.accumulo.manager.metrics;

import java.util.Map;

import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.metrics.MetricsUtil;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.manager.compaction.queue.CompactionJobPriorityQueue;
import org.apache.accumulo.manager.compaction.queue.CompactionJobQueues;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;

public class QueueMetrics implements MetricsProducer {
  private final CompactionJobQueues compactionJobQueues;

  public QueueMetrics(final CompactionJobQueues compactionJobQueues) {
    this.compactionJobQueues = compactionJobQueues;
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    Gauge
        .builder(METRICS_COMPACTOR_JOB_PRIORITY_QUEUES, compactionJobQueues, v -> v.getQueueCount())
        .description("Number of priority queues").tags(MetricsUtil.getCommonTags())
        .register(registry);

    for (Map.Entry<CompactionExecutorId,CompactionJobPriorityQueue> entry : compactionJobQueues
        .getQueues()) {
      Gauge.builder(METRICS_COMPACTOR_JOB_PRIORITY_QUEUE_LENGTH, entry, e -> e.getValue().getSize())
          .description("Length of priority queues")
          .tags(Tags.of("queue_id", entry.getKey().toString())).register(registry);

      Gauge
          .builder(METRICS_COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_QUEUED, entry,
              e -> e.getValue().getQueuedJobs())
          .description("Count of current queued jobs")
          .tags(Tags.of("queue_id", entry.getKey().toString())).register(registry);
      Gauge
          .builder(METRICS_COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_REJECTED, entry,
              e -> e.getValue().getRejectedJobs())
          .description("Count of current rejected jobs")
          .tags(Tags.of("queue_id", entry.getKey().toString())).register(registry);
    }
    // Still need to add Job time spent in queue
  }
}
