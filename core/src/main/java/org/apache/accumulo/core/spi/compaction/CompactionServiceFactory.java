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

import java.util.Collection;
import java.util.Set;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;

/**
 * A Factory that returns a CompactionService based on the environment and configuration.
 *
 * @since 4.0.0
 */
public interface CompactionServiceFactory {

  /**
   * Initializer for compaction factory
   *
   * @param env PluginEnv for the environment
   */

  void init(PluginEnvironment env);

  // Use this for a top level groups pull from the compaction-coordinator
  Collection<CompactorGroupId> getCompactorGroupIds(CompactionServiceId csid,
      ServiceEnvironment senv);

  Set<CompactionServiceId> getCompactionServiceIds();

  /**
   * Return the appropriate CompactionPlanner.
   *
   * @param serviceId ID of the desired compaction service
   * @param senv ServiceEnvironment for the compaction service planner
   * @return CompactionPlanner object
   */
  CompactionPlanner getPlanner(CompactionServiceId serviceId, ServiceEnvironment senv);
}
