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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.clientImpl.DurabilityImpl;
import org.junit.jupiter.api.Test;

public class DfsLoggerBatchSyncTest {

  static class SyncTracker {
    final AtomicInteger hsyncCalls = new AtomicInteger(0);
    final AtomicInteger hflushCalls = new AtomicInteger(0);

    void recordHSync() {
      hsyncCalls.incrementAndGet();
    }

    void recordHFlush() {
      hflushCalls.incrementAndGet();
    }

    int totalSyncs() {
      return hsyncCalls.get();
    }

    int totalFlushes() {
      return hflushCalls.get();
    }
  }

  static class BatchSyncDecider {
    private final int batchSyncSize;
    private int batchSyncCount = 0;
    private final SyncTracker tracker;

    BatchSyncDecider(int batchSyncSize, SyncTracker tracker) {
      if (batchSyncSize < 1) {
        throw new IllegalArgumentException("batchSyncSize must be >=1");
      }
      this.batchSyncSize = batchSyncSize;
      this.tracker = tracker;
    }

    Boolean processWork(List<Durability> durabilities) {
      Boolean shouldHSync = null;

      outer: for (Durability d : durabilities) {
        switch (d) {
          case SYNC:
            shouldHSync = true;
            break outer;

          case BATCH_SYNC:
            batchSyncCount++;
            if (batchSyncCount >= batchSyncSize) {
              shouldHSync = true;
              batchSyncCount = 0;
              break outer;
            } else {
              if (shouldHSync == null) {
                shouldHSync = false;
              }
            }
            break;
          case FLUSH:
            if (shouldHSync == null) {
              shouldHSync = false;
            }
            break;
          default:
            throw new IllegalArgumentException("Unexpected: " + d);
        }
      }

      if (shouldHSync != null) {
        if (shouldHSync) {
          tracker.recordHSync();
        } else {
          tracker.recordHFlush();
        }
      }
      return shouldHSync;
    }

    int getBatchSyncCount() {
      return batchSyncCount;
    }
  }

  @Test
  public void testBatchSyncEnumExists() {
    Durability d = Durability.valueOf("BATCH_SYNC");
    assertEquals(Durability.BATCH_SYNC, d);
  }

  @Test
  public void testBatchSyncFromString() {
    Durability d = DurabilityImpl.fromString("BATCH_SYNC");
    assertEquals(Durability.BATCH_SYNC, d,
        "DurabilityImpl.fromString(\"BATCH_SYNC\") should return BATCH_SYNC");
  }

  @Test
  public void testInvalidDurabilityStringThrows() {
    assertThrows(IllegalArgumentException.class,
        () -> DurabilityImpl.fromString("super-duper-sync"));
  }

  @Test
  public void testBatchSyncOrdinalBetweenFlushAndSync() {
    assertTrue(Durability.BATCH_SYNC.ordinal() > Durability.FLUSH.ordinal(),
        "BATCH_SYNC ordinal must be greater than FLUSH ordinal");
    assertTrue(Durability.BATCH_SYNC.ordinal() < Durability.SYNC.ordinal(),
        "BATCH_SYNC ordinal must be greater than SYNC ordinal");
  }

  /**
   * maxDurability(BATCH_SYNC, FLUSH) == BATCH_SYNC. FAILS until DfsLogger.maxDurability handles the
   * new ordinal.
   */
  @Test
  public void testMaxDurabilityBatchSyncBeatsFlush() {
    Durability result = DfsLogger.maxDurability(Durability.BATCH_SYNC, Durability.FLUSH);
    assertEquals(Durability.BATCH_SYNC, result);
  }

  @Test
  public void testMaxDurabilitySyncBeatsBatchSync() {
    Durability result = DfsLogger.maxDurability(Durability.SYNC, Durability.BATCH_SYNC);
    assertEquals(Durability.SYNC, result);
  }

  @Test
  public void testBatchSyncBelowThresholdProducesOnlyFlush() {
    int batchSize = 5;
    SyncTracker tracker = new SyncTracker();
    BatchSyncDecider decider = new BatchSyncDecider(batchSize, tracker);

    for (int i = 0; i < batchSize - 1; i++) {
      Boolean result = decider.processWork(List.of(Durability.BATCH_SYNC));
      assertEquals(false, result,
          "Transaction " + (i + 1) + " (below threshold) should produce hflush");
    }

    assertEquals(0, tracker.totalSyncs(), "No hsync should have been issued yet");
    assertEquals(batchSize - 1, tracker.totalFlushes(), "Should have flushed for each transaction");
    assertEquals(batchSize - 1, decider.getBatchSyncCount(), "Counter should be batchSize-1");
  }

  @Test
  public void testBatchSyncAtThresholdProducesSync() {
    int batchSize = 3;
    SyncTracker tracker = new SyncTracker();
    BatchSyncDecider decider = new BatchSyncDecider(batchSize, tracker);

    for (int i = 0; i < batchSize - 1; i++) {
      decider.processWork(List.of(Durability.BATCH_SYNC));
    }

    Boolean result = decider.processWork(List.of(Durability.BATCH_SYNC));
    assertEquals(true, result, "The Nth BATCH_SYNC must produce an hsync");
    assertEquals(1, tracker.totalSyncs(), "Exactly one hsync should have been issued");
    assertEquals(0, decider.getBatchSyncCount(), "Counter must reset to 0 after sync");
  }

  @Test
  public void testBatchSyncCountAccumulatesAcrossDrainCycles() {
    int batchSize = 10;
    SyncTracker tracker = new SyncTracker();
    BatchSyncDecider decider = new BatchSyncDecider(batchSize, tracker);

    for (int i = 0; i < 9; i++) {
      decider.processWork(List.of(Durability.BATCH_SYNC));
    }
    assertEquals(0, tracker.totalSyncs());
    assertEquals(9, tracker.totalFlushes());

    decider.processWork(List.of(Durability.BATCH_SYNC));
    assertEquals(1, tracker.totalSyncs());

    for (int i = 0; i < 9; i++) {
      decider.processWork(List.of(Durability.BATCH_SYNC));
    }
    assertEquals(1, tracker.totalSyncs());
    assertEquals(18, tracker.totalFlushes());
  }

  @Test
  public void testBatchSyncMultipleItemsInSingleCycle() {
    int batchSize = 5;
    SyncTracker tracker = new SyncTracker();
    BatchSyncDecider decider = new BatchSyncDecider(batchSize, tracker);

    decider.processWork(
        Arrays.asList(Durability.BATCH_SYNC, Durability.BATCH_SYNC, Durability.BATCH_SYNC));

    assertEquals(0, tracker.totalSyncs(), "3 items below threshold=5; no sync yet");
    assertEquals(1, tracker.totalFlushes(), "Single hflush for the drain cycle");
    assertEquals(3, decider.getBatchSyncCount());

    decider.processWork(Arrays.asList(Durability.BATCH_SYNC, Durability.BATCH_SYNC));

    assertEquals(1, tracker.totalSyncs(), "Total of 5 items: sync must fire");
    assertEquals(0, decider.getBatchSyncCount(), "Counter must reset after sync");
  }

  @Test
  public void testSyncOverridesBatchSync() {
    int batchSize = 100;
    SyncTracker tracker = new SyncTracker();
    BatchSyncDecider decider = new BatchSyncDecider(batchSize, tracker);

    Boolean result = decider
        .processWork(Arrays.asList(Durability.BATCH_SYNC, Durability.FLUSH, Durability.SYNC));

    assertEquals(true, result, "SYNC must win over BATCH_SIZE");
    assertEquals(1, tracker.totalSyncs());
    assertEquals(0, tracker.totalFlushes());
  }

  @Test
  public void testSyncOverrideDoesNotResetBatchCounter() {
    int batchSize = 5;
    SyncTracker tracker = new SyncTracker();
    BatchSyncDecider decider = new BatchSyncDecider(batchSize, tracker);

    for (int i = 0; i < 3; i++) {
      decider.processWork(List.of(Durability.BATCH_SYNC));
    }
    assertEquals(3, decider.getBatchSyncCount());

    decider.processWork(Arrays.asList(Durability.BATCH_SYNC, Durability.SYNC));

    assertEquals(4, decider.getBatchSyncCount(),
        "Counter should be 4 (3 prior + 1 in this cycle); SYNC path does not reset it");
    assertEquals(1, tracker.totalSyncs());
  }

  @Test
  public void testFlushAloneStillJustFlushes() {
    SyncTracker tracker = new SyncTracker();
    BatchSyncDecider decider = new BatchSyncDecider(10, tracker);

    for (int i = 0; i < 20; i++) {
      decider.processWork(List.of(Durability.FLUSH));
    }

    assertEquals(0, tracker.totalSyncs(), "Pure FLUSH must never hsync");
    assertEquals(20, tracker.totalFlushes());
  }

  @Test
  public void testBatchSyncSizeOneEquivalentToSync() {
    SyncTracker tracker = new SyncTracker();
    BatchSyncDecider decider = new BatchSyncDecider(1, tracker);

    for (int i = 0; i < 5; i++) {
      Boolean result = decider.processWork(List.of(Durability.BATCH_SYNC));
      assertEquals(true, result, "With batchSyncSize=1, every item must sync");
    }
    assertEquals(5, tracker.totalSyncs());
    assertEquals(0, tracker.totalFlushes());
  }

  @Test
  public void testInvalidBatchSyncSizeZeroThrows() {
    assertThrows(IllegalArgumentException.class, () -> new BatchSyncDecider(0, new SyncTracker()));
  }

  @Test
  public void testInvalidBatchSyncSizeNegativeThrows() {
    assertThrows(IllegalArgumentException.class, () -> new BatchSyncDecider(-5, new SyncTracker()));
  }

  @Test
  public void testBatchSyncBelowThresholdWithMixedFlush() {
    int batchSize = 10;
    SyncTracker tracker = new SyncTracker();
    BatchSyncDecider decider = new BatchSyncDecider(batchSize, tracker);

    decider.processWork(Arrays.asList(Durability.FLUSH, Durability.BATCH_SYNC));

    assertEquals(0, tracker.totalSyncs());
    assertEquals(1, tracker.totalFlushes(), "Mixed FLUSH+BATCH_SYNC below threshold -> hflush");
    assertEquals(1, decider.getBatchSyncCount());
  }

  @Test
  public void testTwoFullBatchSyncPeriods() {
    int batchSize = 4;
    SyncTracker tracker = new SyncTracker();
    BatchSyncDecider decider = new BatchSyncDecider(batchSize, tracker);

    for (int i = 0; i < batchSize; i++) {
      decider.processWork(List.of(Durability.BATCH_SYNC));
    }
    assertEquals(1, tracker.totalSyncs(), "First period should produce 1 sync");

    for (int i = 0; i < batchSize; i++) {
      decider.processWork(List.of(Durability.BATCH_SYNC));
    }
    assertEquals(2, tracker.totalSyncs(), "Second period should produce 1 more sync");
    assertEquals(batchSize * 2 - 2, tracker.totalFlushes(),
        "Each period has (batchSize-1) flushes");
  }

  @Test
  public void testAllDurabilityOrdinalsUnique() {
    Durability[] values = Durability.values();
    long distinctOrdinals = Arrays.stream(values).mapToInt(Durability::ordinal).distinct().count();
    assertEquals(values.length, distinctOrdinals,
        "Every Durability constant must have a unique ordinal");
  }

  @Test
  public void testFullDurabilityOrder() {
    // Don't simplify these tests as they guarantee enum ordering
    assertTrue(Durability.DEFAULT.ordinal() < Durability.NONE.ordinal());
    assertTrue(Durability.NONE.ordinal() < Durability.LOG.ordinal());
    assertTrue(Durability.LOG.ordinal() < Durability.FLUSH.ordinal());
    assertTrue(Durability.FLUSH.ordinal() < Durability.BATCH_SYNC.ordinal());
    assertTrue(Durability.BATCH_SYNC.ordinal() < Durability.SYNC.ordinal());
  }
}
