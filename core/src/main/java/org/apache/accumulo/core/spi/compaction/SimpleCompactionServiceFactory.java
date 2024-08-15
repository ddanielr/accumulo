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

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.util.compaction.CompactionPlannerInitParams;
import org.apache.accumulo.core.util.compaction.CompactionServicesConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleCompactionServiceFactory implements CompactionServiceFactory {
  private static final Logger log = LoggerFactory.getLogger(SimpleCompactionServiceFactory.class);

  private Supplier<CompactionServicesConfig> factoryConfig;

  @Override
  public void init(PluginEnvironment env) {
    log.info("SCF: INIT Called in SimpleCompactionFactory");
    factoryConfig = env.getConfiguration().getDerived(CompactionServicesConfig::new);

  }

  @Override
  public Collection<CompactorGroupId> getCompactorGroupIds(CompactionServiceId csid,
      ServiceEnvironment senv) {
    var config = factoryConfig.get();
    Objects.requireNonNull(config, "Factory Config has not been initialized");
    HashSet<CompactorGroupId> groupIds = new HashSet<>();

    for (var entry : config.getPlanners().entrySet()) {
      String serviceId = entry.getKey();
      String plannerClassName = entry.getValue();

      log.info("Service id: {}, planner class:{}", serviceId, plannerClassName);

      var initParams = new CompactionPlannerInitParams(CompactionServiceId.of(serviceId),
          config.getPlannerPrefix(serviceId), config.getOptions().get(serviceId), senv);

      try {
        Class<? extends CompactionPlanner> plannerClass =
            Class.forName(plannerClassName).asSubclass(CompactionPlanner.class);
        CompactionPlanner planner = plannerClass.getDeclaredConstructor().newInstance();

        planner.init(initParams);

        initParams.getRequestedGroups().forEach(
            (groupId -> log.info("Compaction service '{}' requested with compactor group '{}'",
                serviceId, groupId)));
      } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException
          | IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException("Failed to load planner : " + plannerClassName);
      }
      groupIds.addAll(initParams.getRequestedGroups());
    }
    return groupIds;
  }

  @Override
  public Set<CompactionServiceId> getCompactionServiceIds() {
    var config = factoryConfig.get();
    Objects.requireNonNull(config, "Factory Config has not been initialized");
    HashSet<CompactionServiceId> serviceIds = new HashSet<>();

    for (var entry : config.getPlanners().entrySet()) {
      String serviceId = entry.getKey();
      String plannerClassName = entry.getValue();

      log.info("Service id: {}, planner class:{}", serviceId, plannerClassName);
      serviceIds.add(CompactionServiceId.of(serviceId));
    }
    return serviceIds;
  }

  @Override
  public CompactionPlanner getPlanner(CompactionServiceId serviceId, ServiceEnvironment senv) {
    var config = factoryConfig.get();
    Objects.requireNonNull(config, "Factory Config has not been initialized");
    // Fail Fast
    if (!config.getPlanners().containsKey(serviceId.canonical())) {
      log.error("Compaction service {} does not exist", serviceId);
      return new ProvisionalCompactionPlanner(serviceId);
    }

    String plannerClassName = config.getPlanners().get(serviceId.canonical());
    log.info("Service id: {}, planner class:{}", serviceId, plannerClassName);

    var options = config.getPlannerPrefix(serviceId.canonical());

    var initParams =
        new CompactionPlannerInitParams(serviceId, config.getPlannerPrefix(serviceId.canonical()),
            config.getOptions().get(serviceId.canonical()), senv);

    CompactionPlanner planner;
    try {
      Class<? extends CompactionPlanner> plannerClass =
          Class.forName(plannerClassName).asSubclass(CompactionPlanner.class);
      planner = plannerClass.getDeclaredConstructor().newInstance();

      planner.init(initParams);

      initParams.getRequestedGroups().forEach(
          (groupId -> log.info("Compaction service '{}' requested with compactor group '{}'",
              serviceId, groupId)));
    } catch (Exception e) {
      log.error(
          "Failed to create compaction planner for {} using class:{} options:{}.  Compaction service will not start any new compactions until its configuration is fixed.",
          serviceId, config.getOptions().get(serviceId.canonical()), options, e);
      planner = new ProvisionalCompactionPlanner(serviceId);
    }

    return planner;
  }
}
