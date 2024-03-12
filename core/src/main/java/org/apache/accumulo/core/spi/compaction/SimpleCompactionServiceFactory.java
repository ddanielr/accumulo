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
import static org.apache.accumulo.core.conf.Property.MANAGER_COMPACTION_SERVICE_PRIORITY_QUEUE_INITIAL_SIZE;

import static org.apache.accumulo.core.util.LazySingletons.GSON;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.util.compaction.CompactionGroupConfig;
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
  private final String plannerClassName = RatioBasedCompactionPlanner.class.getName();
  private final Map<CompactionServiceId,Map<String,String>> serviceOpts = new HashMap<>();
  private final Map<CompactorGroupId,CompactionGroupConfig> compactionGroups = new HashMap<>();

  private static class ServiceConfig {
    String maxOpenFilesPerJob;
    JsonArray groups;
  }

  private static class GroupConfig {
    String group;
    String maxJobs;
    String maxSize;
  }

  @Override
  public void init(PluginEnvironment env) {
    this.env = env;
    var config = env.getConfiguration();
    String factoryConfig = config.get(COMPACTION_SERVICE_FACTORY_CONFIG.getKey());
    String defaultQueueSize = config.get(MANAGER_COMPACTION_SERVICE_PRIORITY_QUEUE_INITIAL_SIZE.getKey());

    // Generate a list of fields from the desired object.
    final List<String> serviceFields = Arrays.stream(ServiceConfig.class.getDeclaredFields())
        .map(Field::getName).collect(Collectors.toList());
    final List<String> groupFields = Arrays.stream(GroupConfig.class.getDeclaredFields())
        .map(Field::getName).collect(Collectors.toList());

    // Each Service is a unique key, so get the keySet to correctly name the service.
    var servicesMap = GSON.get().fromJson(factoryConfig, JsonObject.class);
    Set<Map.Entry<String,JsonElement>> entrySet = servicesMap.entrySet();

    // Find each service in the map and validate its fields
    for (Map.Entry<String,JsonElement> entry : entrySet) {
      Map<String,String> options = new HashMap<>();
      CompactionServiceId csid = CompactionServiceId.of(entry.getKey());
      Preconditions.checkArgument(!serviceOpts.containsKey(csid),
          "Duplicate compaction service definition for service: " + entry.getKey());

      validateConfig(entry.getValue(), serviceFields, ServiceConfig.class.getName());
      ServiceConfig serviceConfig = GSON.get().fromJson(entry.getValue(), ServiceConfig.class);
      var groups = Objects.requireNonNull(serviceConfig.groups,
          "At least one group must be defined for compaction service: " + csid);

      options.put("maxOpen", serviceConfig.maxOpenFilesPerJob);

      // validate the groups defined for the service
      for (JsonElement element : GSON.get().fromJson(groups, JsonArray.class)) {
        validateConfig(element, groupFields, GroupConfig.class.getName());
        GroupConfig groupConfig = GSON.get().fromJson(element, GroupConfig.class);

        String groupName = Objects.requireNonNull(groupConfig.group, "'group' must be specified");
        Integer maxJobs =
            Integer.valueOf(groupConfig.maxJobs == null ? defaultQueueSize : groupConfig.maxJobs);

        var cgid = CompactorGroupId.of(groupName);
        // Check if the compaction service has been defined before
        if (compactionGroups.containsKey(cgid)) {
          throw new IllegalArgumentException(
              "Duplicate compaction group definition on service :" + csid);
        }
        compactionGroups.put(cgid, new CompactionGroupConfig(cgid, maxJobs));
      }
      options.put("groups", GSON.get().toJson(groups));
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

  @Override
  public Set<CompactionGroupConfig> getCompactionGroupConfigs() {
    return new HashSet<>(compactionGroups.values());
  }

  @Override
  public Set<CompactionServiceId> getCompactionServiceIds() {
    return serviceOpts.keySet();
  }

  @Override
  public CompactionPlanner forService(CompactionServiceId serviceId) {
    if (!serviceOpts.containsKey(serviceId)) {
      log.error("Compaction service {} does not exist", serviceId);
      return new ProvisionalCompactionPlanner(serviceId);
    }
    var options = serviceOpts.get(serviceId);
    var initParams = new CompactionPlannerInitParams(options);

    CompactionPlanner planner;
    try {
      planner = env.instantiate(plannerClassName, CompactionPlanner.class);
      planner.init(initParams);
    } catch (Exception e) {
      log.error(
          "Failed to create compaction planner for {} using class:{} options:{}.  Compaction service will not start any new compactions until its configuration is fixed.",
          serviceId, plannerClassName, options, e);
      planner = new ProvisionalCompactionPlanner(serviceId);
    }
    return planner;
  }
}
