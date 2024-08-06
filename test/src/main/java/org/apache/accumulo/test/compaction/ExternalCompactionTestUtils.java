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
package org.apache.accumulo.test.compaction;

import static org.apache.accumulo.core.Constants.DEFAULT_RESOURCE_GROUP_NAME;
import static org.apache.accumulo.core.conf.Property.COMPACTION_SERVICE_FACTORY_CONFIG;
import static org.apache.accumulo.core.util.LazySingletons.GSON;
import static org.apache.accumulo.core.util.LazySingletons.RANDOM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService;
import org.apache.accumulo.core.compaction.thrift.TCompactionState;
import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.compaction.thrift.TExternalCompactionList;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.spi.compaction.CompactionGroup;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactionPlan;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner;
import org.apache.accumulo.core.spi.compaction.CompactionServiceFactory;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.core.spi.compaction.CompactorGroupId;
import org.apache.accumulo.core.spi.compaction.ProvisionalCompactionPlanner;
import org.apache.accumulo.core.spi.compaction.RatioBasedCompactionPlanner;
import org.apache.accumulo.core.spi.compaction.SimpleCompactionDispatcher;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.compaction.CompactionPlannerInitParams;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.compaction.ExternalCompaction_1_IT.TestFilter;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.internal.Maps;
import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

public class ExternalCompactionTestUtils {

  public static final int MAX_DATA = 1000;
  public static final String GROUP1 = "DCQ1";
  public static final String GROUP2 = "DCQ2";
  public static final String GROUP3 = "DCQ3";
  public static final String GROUP4 = "DCQ4";
  public static final String GROUP5 = "DCQ5";
  public static final String GROUP6 = "DCQ6";
  public static final String GROUP7 = "DCQ7";
  public static final String GROUP8 = "DCQ8";

  public static String row(int r) {
    return String.format("r:%04d", r);
  }

  public static void compact(final AccumuloClient client, String table1, int modulus,
      String expectedQueue, boolean wait)
      throws AccumuloSecurityException, TableNotFoundException, AccumuloException {
    IteratorSetting iterSetting = new IteratorSetting(100, TestFilter.class);
    // make sure iterator options make it to compactor process
    iterSetting.addOption("expectedQ", expectedQueue);
    iterSetting.addOption("modulus", modulus + "");
    CompactionConfig config =
        new CompactionConfig().setIterators(List.of(iterSetting)).setWait(wait);
    client.tableOperations().compact(table1, config);
  }

  public static void createTable(AccumuloClient client, String tableName, String service)
      throws Exception {
    Map<String,String> props =
        Map.of("table.compaction.dispatcher", SimpleCompactionDispatcher.class.getName(),
            "table.compaction.dispatcher.opts.service", service);
    NewTableConfiguration ntc = new NewTableConfiguration().setProperties(props);

    client.tableOperations().create(tableName, ntc);

  }

  public static void createTable(AccumuloClient client, String tableName, String service,
      int numTablets) throws Exception {
    SortedSet<Text> splits = new TreeSet<>();
    int jump = MAX_DATA / numTablets;

    for (int r = jump; r < MAX_DATA; r += jump) {
      splits.add(new Text(row(r)));
    }

    createTable(client, tableName, service, splits);
  }

  public static void createTable(AccumuloClient client, String tableName, String service,
      SortedSet<Text> splits) throws Exception {
    Map<String,String> props =
        Map.of("table.compaction.dispatcher", SimpleCompactionDispatcher.class.getName(),
            "table.compaction.dispatcher.opts.service", service);
    NewTableConfiguration ntc = new NewTableConfiguration().setProperties(props).withSplits(splits);

    client.tableOperations().create(tableName, ntc);

  }

  public static void writeData(AccumuloClient client, String table1, int rows)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    try (BatchWriter bw = client.createBatchWriter(table1)) {
      for (int i = 0; i < rows; i++) {
        Mutation m = new Mutation(row(i));
        m.put("", "", "" + i);
        bw.addMutation(m);
      }
    }

    client.tableOperations().flush(table1);
  }

  public static void writeData(AccumuloClient client, String table1)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    writeData(client, table1, MAX_DATA);
  }

  public static void verify(AccumuloClient client, String table1, int modulus)
      throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    verify(client, table1, modulus, MAX_DATA);
  }

  public static void verify(AccumuloClient client, String table1, int modulus, int rows)
      throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    try (Scanner scanner = client.createScanner(table1)) {
      int count = 0;
      for (Entry<Key,Value> entry : scanner) {
        assertEquals(0, Integer.parseInt(entry.getValue().toString()) % modulus,
            String.format("%s %s %d != 0", entry.getValue(), "%", modulus));
        count++;
      }

      int expectedCount = 0;
      for (int i = 0; i < rows; i++) {
        if (i % modulus == 0) {
          expectedCount++;
        }
      }

      assertEquals(expectedCount, count);
    }
  }

  public static void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {

    // ecomp writes from the TabletServer are not being written to the metadata
    // table, they are being queued up instead.
    Map<String,String> clProps = Maps.newHashMap();
    clProps.put(ClientProperty.BATCH_WRITER_LATENCY_MAX.getKey(), "2s");
    cfg.setClientProps(clProps);

    // configure the compaction services to use the queues

    cfg.setProperty(COMPACTION_SERVICE_FACTORY_CONFIG.getKey(), "{ \"default\": {\"planner\": \""
        + RatioBasedCompactionPlanner.class.getName()
        + "\", \"opts\": {\"maxOpenFilesPerJob\": \"30\"}, \"groups\": [{\"group\": \""
        + DEFAULT_RESOURCE_GROUP_NAME + "\", \"opts\": { \"maxSize\": \"128M\"}}]},"
        + "\"cs1\" : {\"planner\": \"" + RatioBasedCompactionPlanner.class.getName() + "\","
        + "\"opts\": {\"maxOpenFilesPerJob\": \"30\"}, \"groups\": [{\"group\": \"" + GROUP1
        + "\"}]}, \"cs2\" : { \"planner\": \"" + RatioBasedCompactionPlanner.class.getName() + "\","
        + "\"opts\": {\"maxOpenFilesPerJob\": \"30\"}, \"groups\": [{\"group\": \"" + GROUP2
        + "\"}]}, \"cs3\" : { \"planner\": \"" + RatioBasedCompactionPlanner.class.getName() + "\","
        + "\"opts\": {\"maxOpenFilesPerJob\": \"30\"}, \"groups\": [{\"group\": \"" + GROUP3
        + "\"}]}, \"cs4\" : { \"planner\": \"" + RatioBasedCompactionPlanner.class.getName() + "\","
        + "\"opts\": {\"maxOpenFilesPerJob\": \"30\"}, \"groups\": [{\"group\": \"" + GROUP4
        + "\"}]}, \"cs5\" : { \"planner\": \"" + RatioBasedCompactionPlanner.class.getName() + "\","
        + "\"opts\": {\"maxOpenFilesPerJob\": \"30\"}, \"groups\": [{\"group\": \"" + GROUP5
        + "\"}]}, \"cs6\" : { \"planner\": \"" + RatioBasedCompactionPlanner.class.getName() + "\","
        + "\"opts\": {\"maxOpenFilesPerJob\": \"30\"}, \"groups\": [{\"group\": \"" + GROUP6
        + "\"}]}, \"cs7\" : { \"planner\": \"" + RatioBasedCompactionPlanner.class.getName() + "\","
        + "\"opts\": {\"maxOpenFilesPerJob\": \"30\"}, \"groups\": [{\"group\": \"" + GROUP7
        + "\"}]}, \"cs8\" : { \"planner\": \"" + RatioBasedCompactionPlanner.class.getName() + "\","
        + "\"opts\": {\"maxOpenFilesPerJob\": \"30\"}, \"groups\": [{\"group\": \"" + GROUP8
        + "\"}]}}");

    cfg.setProperty(Property.COMPACTION_COORDINATOR_FINALIZER_COMPLETION_CHECK_INTERVAL, "5s");
    cfg.setProperty(Property.COMPACTION_COORDINATOR_DEAD_COMPACTOR_CHECK_INTERVAL, "5s");
    cfg.setProperty(Property.COMPACTION_COORDINATOR_TSERVER_COMPACTION_CHECK_INTERVAL, "3s");
    cfg.setProperty(Property.COMPACTOR_CANCEL_CHECK_INTERVAL, "5s");
    cfg.setProperty(Property.COMPACTOR_PORTSEARCH, "true");
    cfg.setProperty(Property.COMPACTOR_MIN_JOB_WAIT_TIME, "100ms");
    cfg.setProperty(Property.COMPACTOR_MAX_JOB_WAIT_TIME, "1s");
    cfg.setProperty(Property.GENERAL_THREADPOOL_SIZE, "10");
    cfg.setProperty(Property.MANAGER_FATE_THREADPOOL_SIZE, "10");
    cfg.setProperty(Property.MANAGER_TABLET_GROUP_WATCHER_INTERVAL, "1s");
    // use raw local file system so walogs sync and flush will work
    coreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  public static TExternalCompactionList getRunningCompactions(ClientContext context,
      Optional<HostAndPort> coordinatorHost) throws TException {
    CompactionCoordinatorService.Client client =
        ThriftUtil.getClient(ThriftClientTypes.COORDINATOR, coordinatorHost.orElseThrow(), context);
    try {
      TExternalCompactionList running =
          client.getRunningCompactions(TraceUtil.traceInfo(), context.rpcCreds());
      return running;
    } finally {
      ThriftUtil.returnClient(client, context);
    }
  }

  private static TExternalCompactionList getCompletedCompactions(ClientContext context,
      Optional<HostAndPort> coordinatorHost) throws Exception {
    CompactionCoordinatorService.Client client =
        ThriftUtil.getClient(ThriftClientTypes.COORDINATOR, coordinatorHost.orElseThrow(), context);
    try {
      TExternalCompactionList completed =
          client.getCompletedCompactions(TraceUtil.traceInfo(), context.rpcCreds());
      return completed;
    } finally {
      ThriftUtil.returnClient(client, context);
    }
  }

  public static TCompactionState getLastState(TExternalCompaction status) {
    ArrayList<Long> timestamps = new ArrayList<>(status.getUpdates().size());
    status.getUpdates().keySet().forEach(k -> timestamps.add(k));
    Collections.sort(timestamps);
    return status.getUpdates().get(timestamps.get(timestamps.size() - 1)).getState();
  }

  public static Set<ExternalCompactionId> waitForCompactionStartAndReturnEcids(ServerContext ctx,
      TableId tid) {
    Set<ExternalCompactionId> ecids = new HashSet<>();
    do {
      try (TabletsMetadata tm =
          ctx.getAmple().readTablets().forTable(tid).fetch(ColumnType.ECOMP).build()) {
        tm.stream().flatMap(t -> t.getExternalCompactions().keySet().stream()).forEach(ecids::add);
      }
      if (ecids.isEmpty()) {
        UtilWaitThread.sleep(50);
      }
    } while (ecids.isEmpty());
    return ecids;
  }

  public static long countTablets(ServerContext ctx, String tableName,
      Predicate<TabletMetadata> tabletTest) {
    var tableId = TableId.of(ctx.tableOperations().tableIdMap().get(tableName));
    try (var tabletsMetadata = ctx.getAmple().readTablets().forTable(tableId).build()) {
      return tabletsMetadata.stream().filter(tabletTest).count();
    }
  }

  public static void waitForRunningCompactions(ServerContext ctx, TableId tid,
      Set<ExternalCompactionId> idsToWaitFor) throws Exception {

    Wait.waitFor(() -> {
      Set<ExternalCompactionId> seen;
      try (TabletsMetadata tm =
          ctx.getAmple().readTablets().forTable(tid).fetch(ColumnType.ECOMP).build()) {
        seen = tm.stream().flatMap(t -> t.getExternalCompactions().keySet().stream())
            .collect(Collectors.toSet());
      }

      return Collections.disjoint(seen, idsToWaitFor);
    });
  }

  public static int confirmCompactionRunning(ServerContext ctx, Set<ExternalCompactionId> ecids)
      throws Exception {
    int matches = 0;
    Optional<HostAndPort> coordinatorHost = ExternalCompactionUtil.findCompactionCoordinator(ctx);
    if (coordinatorHost.isEmpty()) {
      throw new TTransportException("Unable to get CompactionCoordinator address from ZooKeeper");
    }
    while (matches == 0) {
      TExternalCompactionList running =
          ExternalCompactionTestUtils.getRunningCompactions(ctx, coordinatorHost);
      if (running.getCompactions() != null) {
        for (ExternalCompactionId ecid : ecids) {
          TExternalCompaction tec = running.getCompactions().get(ecid.canonical());
          if (tec != null && tec.getUpdates() != null && !tec.getUpdates().isEmpty()) {
            matches++;
            assertEquals(TCompactionState.STARTED, ExternalCompactionTestUtils.getLastState(tec));
          }
        }
      }
      if (matches == 0) {
        UtilWaitThread.sleep(50);
      }
    }
    return matches;
  }

  public static void confirmCompactionCompleted(ServerContext ctx, Set<ExternalCompactionId> ecids,
      TCompactionState expectedState) throws Exception {
    Optional<HostAndPort> coordinatorHost = ExternalCompactionUtil.findCompactionCoordinator(ctx);
    if (coordinatorHost.isEmpty()) {
      throw new TTransportException("Unable to get CompactionCoordinator address from ZooKeeper");
    }

    // The running compaction should be removed
    TExternalCompactionList running =
        ExternalCompactionTestUtils.getRunningCompactions(ctx, coordinatorHost);
    while (running.getCompactions() != null && running.getCompactions().keySet().stream()
        .anyMatch((e) -> ecids.contains(ExternalCompactionId.of(e)))) {
      running = ExternalCompactionTestUtils.getRunningCompactions(ctx, coordinatorHost);
    }
    // The compaction should be in the completed list with the expected state
    TExternalCompactionList completed =
        ExternalCompactionTestUtils.getCompletedCompactions(ctx, coordinatorHost);
    while (completed.getCompactions() == null) {
      UtilWaitThread.sleep(50);
      completed = ExternalCompactionTestUtils.getCompletedCompactions(ctx, coordinatorHost);
    }
    for (ExternalCompactionId e : ecids) {
      TExternalCompaction tec = completed.getCompactions().get(e.canonical());
      assertNotNull(tec);
      assertEquals(expectedState, ExternalCompactionTestUtils.getLastState(tec));
    }

  }

  public static void assertNoCompactionMetadata(ServerContext ctx, String tableName) {
    var tableId = TableId.of(ctx.tableOperations().tableIdMap().get(tableName));
    try (var tabletsMetadata = ctx.getAmple().readTablets().forTable(tableId).build()) {
      assertNoCompactionMetadata(tabletsMetadata);
    }
  }

  public static void assertNoCompactionMetadata(TabletsMetadata tabletsMetadata) {
    int count = 0;

    for (var tabletMetadata : tabletsMetadata) {
      assertNoCompactionMetadata(tabletMetadata);
      count++;
    }

    assertTrue(count > 0);
  }

  public static void assertNoCompactionMetadata(TabletMetadata tabletMetadata) {
    assertEquals(Set.of(), tabletMetadata.getCompacted());
    assertNull(tabletMetadata.getSelectedFiles());
    assertEquals(Set.of(), tabletMetadata.getExternalCompactions().keySet());
    assertEquals(Set.of(), tabletMetadata.getUserCompactionsRequested());
  }

  public static class TestPlanner implements CompactionPlanner {

    private int filesPerCompaction;
    private List<CompactorGroupId> groupIds = new LinkedList<>();
    private EnumSet<CompactionKind> kindsToProcess = EnumSet.noneOf(CompactionKind.class);

    private static class GroupConfig {
      String group;
    }

    @Override
    public void init(InitParameters params) {

      this.filesPerCompaction = Integer.parseInt(params.getOptions().get("filesPerCompaction"));
      for (String kind : params.getOptions().get("process").split(",")) {
        kindsToProcess.add(CompactionKind.valueOf(kind.toUpperCase()));
      }

      for (JsonElement element : GSON.get().fromJson(params.getOptions().get("groups"),
          JsonArray.class)) {
        var groupConfig = GSON.get().fromJson(element, GroupConfig.class);
        var cgid = CompactorGroupId.of(groupConfig.group);
        if (groupIds.contains(cgid)) {
          throw new IllegalStateException("Duplicate group defined");
        }
        groupIds.add(cgid);
      }
    }

    static String getFirstChar(CompactableFile cf) {
      return cf.getFileName().substring(0, 1);
    }

    @Override
    public CompactionPlan makePlan(PlanningParameters params) {
      if (Boolean.parseBoolean(params.getExecutionHints().getOrDefault("compact_all", "false"))) {
        return params
            .createPlanBuilder().addJob((short) 1,
                groupIds.get(RANDOM.get().nextInt(groupIds.size())), params.getCandidates())
            .build();
      }

      if (kindsToProcess.contains(params.getKind())) {
        var planBuilder = params.createPlanBuilder();

        // Group files by first char, like F for flush files or C for compaction produced files.
        // This prevents F and C files from compacting together, which makes it easy to reason about
        // the number of expected files produced by compactions from known number of F files.
        params.getCandidates().stream().collect(Collectors.groupingBy(TestPlanner::getFirstChar))
            .values().forEach(files -> {
              for (int i = filesPerCompaction; i <= files.size(); i += filesPerCompaction) {
                planBuilder.addJob((short) 1, groupIds.get(RANDOM.get().nextInt(groupIds.size())),
                    files.subList(i - filesPerCompaction, i));
              }
            });

        return planBuilder.build();
      } else {
        return params.createPlanBuilder().build();
      }
    }
  }

  public static class TestCompactionServiceFactory implements CompactionServiceFactory {

    private static final Logger log = LoggerFactory
        .getLogger(org.apache.accumulo.core.spi.compaction.SimpleCompactionServiceFactory.class);
    private Map<String,String> plannerOpts = new HashMap<>();
    private final Map<CompactionServiceId,Map<String,String>> serviceOpts = new HashMap<>();
    private final Map<CompactionServiceId,Set<CompactionGroup>> serviceGroups = new HashMap<>();
    private final Set<CompactionGroup> compactionGroups = new HashSet<>();
    private final Map<CompactionServiceId,CompactionPlanner> planners = new HashMap<>();

    private static class ServiceConfig {
      String process;
      String filesPerCompaction;
      JsonArray groups;
    }

    private static class GroupConfig {
      String group;
      JsonObject opts;
    }

    @Override
    public void init(PluginEnvironment env) {
      var config = env.getConfiguration();
      String factoryConfig = config.get(COMPACTION_SERVICE_FACTORY_CONFIG.getKey());

      // Generate a list of fields from the desired object.
      final List<String> serviceFields = Arrays.stream(ServiceConfig.class.getDeclaredFields())
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

        plannerOpts.put("process", serviceConfig.process);
        plannerOpts.put("filesPerCompaction", serviceConfig.filesPerCompaction);

        // validate the groups defined for the service
        mapGroups(csid, groups);
        serviceOpts.put(csid, options);
      }

      for (Map.Entry<CompactionServiceId,Map<String,String>> entry : serviceOpts.entrySet()) {
        var options = entry.getValue();
        Set<CompactionGroup> groups = serviceGroups.get(entry.getKey());
        Objects.requireNonNull(groups, "Compaction groups are not defined for: " + entry.getKey());
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
        planners.putIfAbsent(entry.getKey(), planner);
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
      }
      serviceGroups.put(csid, groups);
      compactionGroups.addAll(groups);
    }

    @Override
    public Set<CompactorGroupId> getCompactorGroupIds() {
      return compactionGroups.stream().map(CompactionGroup::getGroupId).collect(Collectors.toSet());
    }

    @Override
    public Set<CompactionServiceId> getCompactionServiceIds() {
      return serviceOpts.keySet();
    }

    @Override
    public Boolean validate(PluginEnvironment env) {
      return true;
    }

    @Override
    public Collection<CompactionJob> getJobs(CompactionPlanner.PlanningParameters params,
        CompactionServiceId serviceId) {
      try {
        return planners.getOrDefault(serviceId, new ProvisionalCompactionPlanner(serviceId))
            .makePlan(params).getJobs();
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
}
