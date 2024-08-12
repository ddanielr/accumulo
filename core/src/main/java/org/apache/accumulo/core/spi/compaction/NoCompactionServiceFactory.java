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
package org.apache.accumulo.core.spi.compaction;

import static org.apache.accumulo.core.Constants.DEFAULT_RESOURCE_GROUP_NAME;

import java.util.Set;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.data.TableId;

public class NoCompactionServiceFactory implements CompactionServiceFactory {
  public static final CompactionServiceId csid = CompactionServiceId.of("NoCompactionService");
  public static final CompactorGroupId cgid = CompactorGroupId.of(DEFAULT_RESOURCE_GROUP_NAME);
  public static final CompactionGroup compactionGroup = new CompactionGroup(cgid, null);
  public static final CompactionPlanner NONE = new ProvisionalCompactionPlanner(csid);

  /**
   * @param env PluginEnv for the environment
   */
  @Override
  public void init(PluginEnvironment env) {
    // no-op
  }

  /**
   * @return Set of defined compactionGroups
   */
  @Override
  public Set<CompactionGroup> getCompactionGroups(CompactionServiceId csid) {
    return Set.of(compactionGroup);
  }

  /**
   * @return Set of defined CompactionServiceIds
   */
  @Override
  public Set<CompactionServiceId> getCompactionServiceIds() {
    return Set.of(csid);
  }

  /**
   * @param serviceId ID of the desired compaction service
   * @return CompactionPlanner that is set for the specified CompactionServiceId
   */
  @Override
  public CompactionPlanner getPlanner(TableId tableId, CompactionServiceId serviceId) {
    return NONE;
  }
}