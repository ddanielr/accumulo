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
package org.apache.accumulo.core.spi.balancer;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.manager.balancer.AssignmentParamsImpl;
import org.apache.accumulo.core.manager.balancer.BalanceParamsImpl;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.spi.balancer.data.TabletMigration;
import org.apache.accumulo.core.spi.balancer.data.TabletServerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 2.1.0
 */
public class TableLoadBalancer implements TabletBalancer {

  private static final Logger log = LoggerFactory.getLogger(TableLoadBalancer.class);

  protected BalancerEnvironment environment;
  Map<TableId,TabletBalancer> perTableBalancers = new HashMap<>();

  @Override
  public void init(BalancerEnvironment balancerEnvironment) {
    this.environment = balancerEnvironment;
  }

  private TabletBalancer constructNewBalancerForTable(String clazzName, TableId tableId)
      throws Exception {
    String context = environment.tableContext(tableId);
    Class<? extends TabletBalancer> clazz =
        ClassLoaderUtil.loadClass(context, clazzName, TabletBalancer.class);
    Constructor<? extends TabletBalancer> constructor = clazz.getConstructor(TableId.class);
    return constructor.newInstance(tableId);
  }

  protected String getLoadBalancerClassNameForTable(TableId table) {
    if (environment.isTableOnline(table)) {
      return environment.getConfiguration(table).get(Property.TABLE_LOAD_BALANCER.getKey());
    }
    return null;
  }

  protected TabletBalancer getBalancerForTable(TableId tableId) {
    log.info("Fetching Balancer for table {}", tableId);
    TabletBalancer balancer = perTableBalancers.get(tableId);
    log.info("Balancer {} retrieved from cache for table {}", balancer, tableId);

    String clazzName = getLoadBalancerClassNameForTable(tableId);

    if (clazzName == null) {
      clazzName = SimpleLoadBalancer.class.getName();
      log.info("Default Balancer Chosen of {}", clazzName);
    }
    if (balancer != null) {
      if (!clazzName.equals(balancer.getClass().getName())) {
        // the balancer class for this table does not match the class specified in the configuration
        try {
          balancer = constructNewBalancerForTable(clazzName, tableId);
          perTableBalancers.put(tableId, balancer);
          balancer.init(environment);

          log.info("Loaded existing class {} for table {}", clazzName, tableId);
        } catch (Exception e) {
          log.warn("Failed to load table balancer class {} for table {}", clazzName, tableId, e);
        }
      }
    }
    if (balancer == null) {
      try {
        balancer = constructNewBalancerForTable(clazzName, tableId);
        log.info("Loaded new balancer class {} for table {}", clazzName, tableId);
      } catch (Exception e) {
        log.warn("Failed to load table balancer class {} for table {}", clazzName, tableId, e);
      }

      if (balancer == null) {
        log.info("Using balancer {} for table {}", SimpleLoadBalancer.class.getName(), tableId);
        balancer = new SimpleLoadBalancer(tableId);
      }
      perTableBalancers.put(tableId, balancer);
      balancer.init(environment);
    }
    return balancer;
  }

  @Override
  public void getAssignments(AssignmentParameters params) {
    // separate the unassigned into tables
    Map<TableId,Map<TabletId,TabletServerId>> groupedUnassigned = new HashMap<>();
    params.unassignedTablets().forEach((tid, lastTserver) -> groupedUnassigned
        .computeIfAbsent(tid.getTable(), k -> new HashMap<>()).put(tid, lastTserver));
    for (Entry<TableId,Map<TabletId,TabletServerId>> e : groupedUnassigned.entrySet()) {
      Map<TabletId,TabletServerId> newAssignments = new HashMap<>();
      getBalancerForTable(e.getKey()).getAssignments(
          new AssignmentParamsImpl(params.currentStatus(), e.getValue(), newAssignments));
      newAssignments.forEach(params::addAssignment);
    }
  }

  @Override
  public long balance(BalanceParameters params) {
    long minBalanceTime = 5_000;
    // Iterate over the tables and balance each of them
    final DataLevel currentDataLevel = DataLevel.valueOf(params.currentLevel());
    for (TableId tableId : environment.getTableIdMap().values()) {
      if (DataLevel.of(tableId) != currentDataLevel) {
        // Don't try and balance tables that are outside the current data level.
        log.info("Skipping Table {} as it is not in the current data level", tableId);
        continue;
      }
        ArrayList<TabletMigration> newMigrations = new ArrayList<>();
        long tableBalanceTime =
            getBalancerForTable(tableId).balance(new BalanceParamsImpl(params.currentStatus(),
                params.currentMigrations(), newMigrations, currentDataLevel));
        if (tableBalanceTime < minBalanceTime) {
          minBalanceTime = tableBalanceTime;
        }
        log.info("Added new migrations: {}", newMigrations);
        params.migrationsOut().addAll(newMigrations);
    }
    return minBalanceTime;
  }
}
