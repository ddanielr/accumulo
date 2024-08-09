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

import static org.apache.accumulo.core.conf.Property.COMPACTION_SERVICE_FACTORY_CONFIG;
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
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.data.TableId;
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
  private PluginEnvironment env;
  private final Map<CompactionServiceId,Map<String,String>> serviceOpts = new HashMap<>();
  // This feels clever vs having a second Map.
  private final Map<CompactionServiceId,Set<CompactionGroup>> serviceGroups = new HashMap<>();

  private static class ServiceConfig {
    String planner;
    JsonObject opts;
    JsonArray groups;
  }

  private static class GroupConfig {
    String group;
    String maxSize;
  }

  private static class PlannerOpts {
    String maxOpenFilesPerJob;
  }

  @Override
  public void init(PluginEnvironment env) {
    log.info("SCF: INIT Called in SimpleCompactionFactory");
    this.env = env;
    var config = env.getConfiguration();
    String factoryConfig = config.get(COMPACTION_SERVICE_FACTORY_CONFIG.getKey());

    // Generate a list of fields from the desired object.
    final List<String> serviceFields = Arrays.stream(ServiceConfig.class.getDeclaredFields())
        .map(Field::getName).collect(Collectors.toList());

    // Each Service is a unique key, so get the keySet to correctly name the service.
    var servicesMap = GSON.get().fromJson(factoryConfig, JsonObject.class);
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
      options.put("plannerOpts", serviceConfig.opts.toString());

      var groups = Objects.requireNonNull(serviceConfig.groups,
          "At least one group must be defined for compaction service: " + csid);

      // validate the groups defined for the service
      mapGroups(csid, groups);
      log.info("SCF: Adding compaction Service: {}", csid);
      serviceOpts.put(csid, options);
    }

    // TODO:
    // if changes are detected to services,
    // then the compactionGroups and compactionServices objects
    // should be wiped.
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
      Long maxSize = groupConfig.maxSize == null ? null
          : ConfigurationTypeHelper.getFixedMemoryAsBytes(groupConfig.maxSize);

      var cgid = CompactorGroupId.of(groupName);
      // Check if the compaction service has been defined before
      if (serviceGroups.get(csid) != null) {
        var currentGroups = serviceGroups.get(csid);
        if (!currentGroups.isEmpty() && currentGroups.stream().map(CompactionGroup::getGroupId)
            .anyMatch(Predicate.isEqual(cgid))) {
          throw new IllegalArgumentException(
              "Duplicate compaction group definition on service : " + csid);
        }
      }
      var compactionGroup = new CompactionGroup(cgid, maxSize);
      groups.add(compactionGroup);
    }
    serviceGroups.put(csid, groups);
  }

  @Override
  public Collection<CompactionGroup> getCompactionGroups(CompactionServiceId serviceId) {
    Preconditions.checkNotNull(serviceGroups.get(serviceId),
        "Compaction Service " + serviceId + "is not defined");
    return serviceGroups.get(serviceId);
  }

  @Override
  public Set<CompactionServiceId> getCompactionServiceIds() {
    return serviceOpts.keySet();
  }

  @Override
  public CompactionPlanner getPlanner(TableId tableId, CompactionServiceId serviceId) {
    if (!serviceOpts.containsKey(serviceId)) {
      log.error("Compaction service {} does not exist", serviceId);
      return new ProvisionalCompactionPlanner(serviceId);
    }
    var options = serviceOpts.get(serviceId);

    // These get internalized into the planner.
    // Planners require a validation method.
    Set<CompactionGroup> groups = serviceGroups.get(serviceId);
    Preconditions.checkNotNull(groups, "Compaction groups are not defined for: {}", serviceId);

    // Parse the Planner OPTS here vs up in init?
    var plannerOpts = GSON.get().fromJson(options.get("plannerOpts"), PlannerOpts.class);
    var initParams = new CompactionPlannerInitParams(
        Map.of("maxOpenFilesPerJob", plannerOpts.maxOpenFilesPerJob), groups);

    CompactionPlanner planner;
    try {
      planner = env.instantiate(tableId, options.get("planner"), CompactionPlanner.class);
      planner.init(initParams);
    } catch (Exception e) {
      log.error(
          "Failed to create compaction planner for {} using class:{} options:{}.  Compaction service will not start any new compactions until its configuration is fixed.",
          serviceId, options.get("planner"), options, e);
      planner = new ProvisionalCompactionPlanner(serviceId);
    }
    return planner;
  }
}
