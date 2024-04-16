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
package org.apache.accumulo.tserver;

import java.util.List;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metrics.MetricsProducer;

import com.github.benmanes.caffeine.cache.LoadingCache;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;

public class ScanServerMetrics implements MetricsProducer {

  private Timer reservationTimer;
  private Counter busyCounter;

  private final LoadingCache<KeyExtent,TabletMetadata> tabletMetadataCache;

  public ScanServerMetrics(final LoadingCache<KeyExtent,TabletMetadata> tabletMetadataCache) {
    this.tabletMetadataCache = tabletMetadataCache;
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    reservationTimer = Timer.builder(MetricsProducer.METRICS_SSERVER_REGISTRATION_TIMER)
        .description("Time to reserve a tablets files for scan").register(registry);
    busyCounter = Counter.builder(MetricsProducer.METRICS_SSERVER_BUSY_COUNTER)
        .description("The number of scans where a busy timeout happened").register(registry);
    CaffeineCacheMetrics.monitor(registry, tabletMetadataCache,
        METRICS_SSERVER_TABLET_METADATA_CACHE, List.of());
  }

  public Timer getReservationTimer() {
    return reservationTimer;
  }

  public void incrementBusy() {
    busyCounter.increment();
  }
}
