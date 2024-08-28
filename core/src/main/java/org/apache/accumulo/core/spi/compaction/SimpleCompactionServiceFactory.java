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

import static org.apache.accumulo.core.conf.Property.COMPACTION_SERVICE_CONFIG;
import static org.apache.accumulo.core.util.LazySingletons.GSON;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.util.compaction.CompactionPlannerInitParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

public class SimpleCompactionServiceFactory implements CompactionServiceFactory {
  private static final Logger log = LoggerFactory.getLogger(SimpleCompactionServiceFactory.class);

  // private static final Logger UNKNOWN_SERVICE_ERROR_LOG =
  // new ConditionalLogger.EscalatingLogger(log, Duration.ofMinutes(5), 3000, Level.ERROR);
  // private static final Logger PLANNING_INIT_ERROR_LOG =
  // new ConditionalLogger.EscalatingLogger(log, Duration.ofMinutes(5), 3000, Level.ERROR);
  // private static final Logger PLANNING_ERROR_LOG =
  // new ConditionalLogger.EscalatingLogger(log, Duration.ofMinutes(5), 3000, Level.ERROR);

  private Supplier<CompactionServiceConf> factoryConfig;

  private static class PlannerOpts {
    String maxOpenFilesPerJob;
  }

  static class CompactionServiceConf {

    private static class ServiceConfig {
      String planner;
      JsonObject opts;
      JsonArray groups;
    }

    private static class GroupConfig {
      String group;
      JsonObject opts;
    }

    private final Map<CompactionServiceId,Map<String,String>> serviceOpts = new HashMap<>();
    private final Map<CompactionServiceId,Set<CompactionGroup>> serviceGroups = new HashMap<>();
    private final Set<CompactionGroup> compactionGroups = new HashSet<>();
    private final Map<CompactionServiceId,CompactionPlanner> planners = new HashMap<>();

    CompactionServiceConf(PluginEnvironment.Configuration conf) {
      log.info("Building Compaction Services Conf");
      var config = conf.get(COMPACTION_SERVICE_CONFIG.getKey());

      // Generate a list of fields from the desired object.
      final List<String> serviceFields = Arrays.stream(ServiceConfig.class.getDeclaredFields())
          .map(Field::getName).collect(Collectors.toList());

      // Each Service is a unique key, so get the keySet to correctly name the service.
      var servicesMap = GSON.get().fromJson(config, JsonObject.class);
      Set<Map.Entry<String,JsonElement>> entrySet = servicesMap.entrySet();

      log.info("SCF: Processing Entries");
      // Find each service in the map and validate its fields
      for (Map.Entry<String,JsonElement> entry : entrySet) {
        Map<String,String> options = new HashMap<>();
        CompactionServiceId csid = CompactionServiceId.of(entry.getKey());
        log.info("SCF: Processing of compaction service: {}", csid);
        Preconditions.checkArgument(!serviceOpts.containsKey(csid),
            "Duplicate compaction service definition for service: " + entry.getKey());

        validateConfig(entry.getValue(), serviceFields, ServiceConfig.class.getName());
        ServiceConfig serviceConfig = GSON.get().fromJson(entry.getValue(), ServiceConfig.class);

        var planner = Objects.requireNonNull(serviceConfig.planner,
            "A compaction planner must be defined for the compaction service: " + csid);
        // Validate that planner is a class here
        options.put("planner", planner);
        // Validation for these opts has to be moved down to the planner creation stage.
        if (serviceConfig.opts != null) {
          options.put("plannerOpts", serviceConfig.opts.toString());
        }

        var groups = Objects.requireNonNull(serviceConfig.groups,
            "At least one group must be defined for compaction service: " + csid);

        // validate the groups defined for the service
        mapGroups(csid, groups);
        log.info("SCF: Adding compaction Service: {}", csid);
        serviceOpts.put(csid, options);
      }
    }

    private void validateConfig(JsonElement json, List<String> fields, String className) {

      JsonObject jsonObject = GSON.get().fromJson(json, JsonObject.class);

      List<String> objectProperties = new ArrayList<>(jsonObject.keySet());
      HashSet<String> classFieldNames = new HashSet<>(fields);

      if (!classFieldNames.containsAll(objectProperties)) {
        objectProperties.removeAll(classFieldNames);
        throw new JsonParseException(
            "Invalid fields: " + objectProperties + " provided for class: " + className);
      }
    }

    private void mapGroups(CompactionServiceId csid, JsonArray groupArray) {
      final List<String> groupFields = Arrays.stream(GroupConfig.class.getDeclaredFields())
          .map(Field::getName).collect(Collectors.toList());

      HashSet<CompactionGroup> groups = new HashSet<>();

      for (JsonElement element : GSON.get().fromJson(groupArray, JsonArray.class)) {
        validateConfig(element, groupFields, GroupConfig.class.getName());
        GroupConfig groupConfig = GSON.get().fromJson(element, GroupConfig.class);

        String groupName = Objects.requireNonNull(groupConfig.group, "'group' must be specified");

        Long maxSize = null;
        // Parse for various group options
        if (groupConfig.opts != null) {
          for (Map.Entry<String,JsonElement> entry : groupConfig.opts.entrySet()) {
            if (Objects.equals(entry.getKey(), "maxSize")) {
              maxSize =
                  ConfigurationTypeHelper.getFixedMemoryAsBytes(entry.getValue().getAsString());
            }
          }
        }

        var cgid = CompactorGroupId.of(groupName);
        // Check if the compaction service has been defined before
        if (!compactionGroups.isEmpty() && compactionGroups.stream()
            .map(CompactionGroup::getGroupId).anyMatch(Predicate.isEqual(cgid))) {
          throw new IllegalArgumentException(
              "Duplicate compaction group definition on service : " + csid);
        }
        var compactionGroup = new CompactionGroup(cgid, maxSize);
        groups.add(compactionGroup);
        compactionGroups.add(compactionGroup);
      }
      serviceGroups.put(csid, groups);
    }
  }

  @Override
  public void init(PluginEnvironment env) {
    log.info("SCF: INIT Called in SimpleCompactionFactory");
    this.factoryConfig = env.getConfiguration().getDerived(CompactionServiceConf::new);
    // Maybe we don't use the planner map and instead just directly call the derived config?
    validatePlanners(env, this.factoryConfig);
  }

  /**
   * validate planners from a given config.
   *
   * @param env environment used to create the planners
   * @param config configuration that is parsed from the property value
   */

  private static void validatePlanners(PluginEnvironment env,
      Supplier<CompactionServiceConf> config) {
    for (Map.Entry<CompactionServiceId,Map<String,String>> entry : config.get().serviceOpts
        .entrySet()) {
      var options = entry.getValue();
      Set<CompactionGroup> groups = config.get().serviceGroups.get(entry.getKey());
      Objects.requireNonNull(groups, "Compaction groups are not defined for: " + entry.getKey());

      HashMap<String,String> plannerOpts = new HashMap<>();

      if (options.get("plannerOpts") != null) {
        var plannerOptsClass = GSON.get().fromJson(options.get("plannerOpts"), PlannerOpts.class);
        plannerOpts.put("maxOpenFilesPerJob", plannerOptsClass.maxOpenFilesPerJob);
      }
      var initParams = new CompactionPlannerInitParams(plannerOpts, groups);
      CompactionPlanner planner;
      try {
        planner = env.instantiate(options.get("planner"), CompactionPlanner.class);
        planner.init(initParams);
      } catch (Exception e) {
        log.error(
            "Failed to create compaction planner for {} using class:{} options:{}.  Compaction service will not start any new compactions until its configuration is fixed.",
            entry.getKey(), options.get("planner"), options, e);
        planner = new ProvisionalCompactionPlanner(entry.getKey());
      }
      config.get().planners.putIfAbsent(entry.getKey(), planner);
    }
  }

  @Override
  public Set<CompactionServiceId> getCompactionServiceIds() {

    Objects.requireNonNull(factoryConfig.get(), "Factory Config has not been initialized");
    if (factoryConfig.get().planners.isEmpty()) {
      return Set.of();
    }
    return factoryConfig.get().planners.keySet();
  }

  @Override
  public Set<CompactorGroupId> getCompactorGroupIds() {
    return this.factoryConfig.get().compactionGroups.stream().map(CompactionGroup::getGroupId)
        .collect(Collectors.toSet());
  }

  @Override
  public Boolean validate(PluginEnvironment env) {
    Supplier<CompactionServiceConf> config;
    try {
      config = env.getConfiguration().getDerived(CompactionServiceConf::new);
      validatePlanners(env, config);
    } catch (Exception e) {
      log.error("Property {} failed validation with class {}", COMPACTION_SERVICE_CONFIG,
          this.getClass().getName(), e);
      return false;
    }
    if (config.get() != null && config.get().planners.isEmpty()) {
      log.warn("No valid planners were created from the config");
    }
    return true;
  }

  @Override
  public Collection<CompactionJob> getJobs(CompactionPlanner.PlanningParameters params,
      CompactionServiceId serviceId) {
    try {
      return factoryConfig.get().planners
          .getOrDefault(serviceId, new ProvisionalCompactionPlanner(serviceId)).makePlan(params)
          .getJobs();
    } catch (Exception e) {
      // PLANNING_ERROR_LOG.trace(
      log.trace(
          "Failed to plan compactions for service:{} kind:{} tableId:{} hints:{}.  Compaction service may not start any"
              + " new compactions until this issue is resolved. Duplicates of this log message are temporarily"
              + " suppressed.",
          serviceId, params.getKind(), params.getTableId(), params.getExecutionHints(), e);
      return Set.of();
    }
  }
}
