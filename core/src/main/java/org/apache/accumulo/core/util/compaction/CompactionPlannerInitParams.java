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

import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.spi.compaction.CompactionGroup;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner;
import org.apache.accumulo.core.spi.compaction.CompactorGroupId;
import org.apache.accumulo.core.spi.compaction.GroupManager;

import com.google.common.base.Preconditions;

public class CompactionPlannerInitParams implements CompactionPlanner.InitParameters {
  private final Map<String,String> plannerOpts;
  private final Collection<CompactionGroup> compactorGroups;

  public CompactionPlannerInitParams(Map<String,String> plannerOpts,
      Collection<CompactionGroup> groups) {
    this.plannerOpts = plannerOpts;
    this.compactorGroups = groups;
  }

  @Override
  public Map<String,String> getOptions() {
    return plannerOpts;
  }

  @Override
  public GroupManager getGroupManager() {
    return new GroupManager() {

      @Override
      public CompactorGroupId getGroup(String name) {
        var cgid = CompactorGroupId.of(name);
        Preconditions.checkArgument(compactorGroups.stream().map(CompactionGroup::getGroupId)
            .noneMatch(id -> id.equals(cgid)), "Duplicate compactor group for group: " + name);
        return cgid;
      }

      @Override
      public Collection<CompactionGroup> getGroups() {
        return compactorGroups;
      }
    };
  }
}
