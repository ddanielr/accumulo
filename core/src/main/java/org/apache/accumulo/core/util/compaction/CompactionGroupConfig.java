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
package org.apache.accumulo.core.util.compaction;

import java.util.Objects;

import org.apache.accumulo.core.spi.compaction.CompactorGroupId;

import com.google.common.base.Preconditions;

public class CompactionGroupConfig {

  private final CompactorGroupId cgid;
  private final int maxJobs;

  /**
   * Defines the concept of a CompactionGroup for the compaction coordinator.
   *
   * @param cgid ID of the compactor group
   * @param maxJobs Size of the priority queue assigned to this group
   */
  public CompactionGroupConfig(CompactorGroupId cgid, Integer maxJobs) {
    Preconditions.checkArgument(maxJobs == null || maxJobs > 0, "Invalid value for maxJobs");
    this.cgid = Objects.requireNonNull(cgid, "Compaction Group ID is null");
    this.maxJobs = maxJobs;
  }

  public CompactorGroupId getGroupId() {
    return cgid;
  }

  public int getMaxJobs() {
    return maxJobs;
  }
}
