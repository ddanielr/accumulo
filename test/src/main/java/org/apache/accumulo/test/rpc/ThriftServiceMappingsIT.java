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
package org.apache.accumulo.test.rpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockData.ThriftService;
import org.apache.accumulo.core.lock.ServiceLockPaths;
import org.apache.accumulo.core.lock.ServiceLockPaths.AddressSelector;
import org.apache.accumulo.core.lock.ServiceLockPaths.ResourceGroupPredicate;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.zookeeper.ZcStat;
import org.apache.accumulo.core.zookeeper.ZooCache;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test discovers all ThriftService values present in zk service lock data and creates a
 * mapping from ThriftService to multiple ServiceLockPaths.
 */
public class ThriftServiceMappingsIT extends AccumuloClusterHarness {

  private static final Logger log = LoggerFactory.getLogger(ThriftServiceMappingsIT.class);

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "10s");
    cfg.getClusterServerConfiguration().setNumDefaultCompactors(1);
    cfg.getClusterServerConfiguration().setNumDefaultScanServers(1);
    cfg.getClusterServerConfiguration().setNumDefaultTabletServers(1);
    // Set two managers so that we get at least one assistant lock without a matching primaryLock
    cfg.getClusterServerConfiguration().setNumManagers(2);
  }

  /**
   * Discovers all ThriftService values that exist in service locks across all running servers.
   */
  @Test
  public void testDiscoverAllThriftServices() {
    ZooCache zc = getServerContext().getZooCache();
    ServiceLockPaths paths = getServerContext().getServerPaths();

    Map<ThriftService,Set<String>> discoveredServices = new HashMap<>();

    checkServiceLockForThriftServices(zc, paths.getManager(true), "Manager", discoveredServices);
    checkServiceLockForThriftServices(zc, paths.getGarbageCollector(true), "GC",
        discoveredServices);
    checkServiceLockForThriftServices(zc, paths.getMonitor(true), "Monitor", discoveredServices);

    Set<ServiceLockPath> assistantManagerServerPaths =
        paths.getAssistantManagers(AddressSelector.all(), true);
    for (ServiceLockPath path : assistantManagerServerPaths) {
      checkServiceLockForThriftServices(zc, path, "Assistant Manager", discoveredServices);
    }

    Set<ServiceLockPath> tserverPaths =
        paths.getTabletServer(ResourceGroupPredicate.ANY, AddressSelector.all(), true);
    for (ServiceLockPath path : tserverPaths) {
      checkServiceLockForThriftServices(zc, path, "TServer", discoveredServices);
    }

    Set<ServiceLockPath> compactorPaths =
        paths.getCompactor(ResourceGroupPredicate.ANY, AddressSelector.all(), true);
    for (ServiceLockPath path : compactorPaths) {
      checkServiceLockForThriftServices(zc, path, "Compactor", discoveredServices);
    }

    Set<ServiceLockPath> scanServerPaths =
        paths.getScanServer(ResourceGroupPredicate.ANY, AddressSelector.all(), true);
    for (ServiceLockPath path : scanServerPaths) {
      checkServiceLockForThriftServices(zc, path, "SServer", discoveredServices);
    }

    log.info("Discovered ThriftServices across all running servers:");
    for (Map.Entry<ThriftService,Set<String>> entry : discoveredServices.entrySet()) {
      log.info("  {} -> found in: {}", entry.getKey(), entry.getValue());
    }

    // Verify we found expected services
    assertTrue(discoveredServices.containsKey(ThriftService.CLIENT),
        "Should have discovered CLIENT service");
    assertTrue(discoveredServices.containsKey(ThriftService.COORDINATOR),
        "Should have discovered COORDINATOR service");
    assertTrue(discoveredServices.containsKey(ThriftService.COMPACTOR),
        "Should have discovered COMPACTOR service");
    assertTrue(discoveredServices.containsKey(ThriftService.FATE_CLIENT),
        "Should have discovered FATE_CLIENT service");
    assertTrue(discoveredServices.containsKey(ThriftService.FATE_WORKER),
        "Should have discovered FATE_WORKER service");
    assertTrue(discoveredServices.containsKey(ThriftService.GC),
        "Should have discovered GC service");
    assertTrue(discoveredServices.containsKey(ThriftService.MANAGER),
        "Should have discovered MANAGER service");
    assertTrue(discoveredServices.containsKey(ThriftService.TABLET_INGEST),
        "Should have discovered TABLET_INGEST service");
    assertTrue(discoveredServices.containsKey(ThriftService.TABLET_MANAGEMENT),
        "Should have discovered TABLET_MANAGEMENT service");
    assertTrue(discoveredServices.containsKey(ThriftService.TABLET_SCAN),
        "Should have discovered TABLET_SCAN service");
    assertTrue(discoveredServices.containsKey(ThriftService.TSERV),
        "Should have discovered TSERV service");

    // Verify services that should NOT be in service locks
    assertFalse(discoveredServices.containsKey(ThriftService.NONE),
        "NONE should not appear in service locks");
  }

  /**
   * Creates and validates a mapping from ThriftService to ServiceLockPath type.
   */
  @Test
  public void testCreateThriftServiceToServiceLockPathMapping() {
    ZooCache zc = getServerContext().getZooCache();
    ServiceLockPaths paths = getServerContext().getServerPaths();

    Map<ThriftService,Set<String>> serviceToPathType = new EnumMap<>(ThriftService.class);

    addMappingsFromLock(zc, paths.getManager(true), serviceToPathType);
    addMappingsFromLock(zc, paths.getGarbageCollector(true), serviceToPathType);

    ServiceLockPath monitorPath = paths.getMonitor(true);
    if (monitorPath != null) {
      addMappingsFromLock(zc, monitorPath, serviceToPathType);
    }

    for (ServiceLockPath path : paths.getAssistantManagers(AddressSelector.all(), true)) {
      addMappingsFromLock(zc, path, serviceToPathType);
    }

    for (ServiceLockPath path : paths.getTabletServer(ResourceGroupPredicate.ANY,
        AddressSelector.all(), true)) {
      addMappingsFromLock(zc, path, serviceToPathType);
    }

    for (ServiceLockPath path : paths.getCompactor(ResourceGroupPredicate.ANY,
        AddressSelector.all(), true)) {
      addMappingsFromLock(zc, path, serviceToPathType);
    }

    for (ServiceLockPath path : paths.getScanServer(ResourceGroupPredicate.ANY,
        AddressSelector.all(), true)) {
      addMappingsFromLock(zc, path, serviceToPathType);
    }

    log.info("ThriftService to ServiceLockPaths:");
    for (Map.Entry<ThriftService,Set<String>> entry : serviceToPathType.entrySet()) {
      log.info("  {} -> {}", entry.getKey(), entry.getValue());
    }

    // Validate the Manager
    validateMapping(serviceToPathType, ThriftService.MANAGER, Set.of(Constants.ZMANAGER_LOCK));
    validateMapping(serviceToPathType, ThriftService.COORDINATOR, Set.of(Constants.ZMANAGER_LOCK));
    validateMapping(serviceToPathType, ThriftService.FATE_CLIENT, Set.of(Constants.ZMANAGER_LOCK));

    // Validate the client
    validateMapping(serviceToPathType, ThriftService.CLIENT,
        Set.of(Constants.ZCOMPACTORS, Constants.ZSSERVERS, Constants.ZTSERVERS));

    // Validate the tserver
    validateMapping(serviceToPathType, ThriftService.TSERV, Set.of(Constants.ZTSERVERS));
    validateMapping(serviceToPathType, ThriftService.TABLET_INGEST, Set.of(Constants.ZTSERVERS));
    validateMapping(serviceToPathType, ThriftService.TABLET_MANAGEMENT,
        Set.of(Constants.ZTSERVERS));

    // Validate tablet scanning
    validateMapping(serviceToPathType, ThriftService.TABLET_SCAN,
        Set.of(Constants.ZSSERVERS, Constants.ZTSERVERS));

    // Handle the rest
    validateMapping(serviceToPathType, ThriftService.GC, Set.of(Constants.ZGC_LOCK));
    validateMapping(serviceToPathType, ThriftService.COMPACTOR, Set.of(Constants.ZCOMPACTORS));
    validateMapping(serviceToPathType, ThriftService.FATE_WORKER,
        Set.of(Constants.ZMANAGER_ASSISTANT_LOCK));

    assertFalse(serviceToPathType.containsKey(ThriftService.NONE));

    log.info("Total ThriftServices discovered: {}", serviceToPathType.size());
    log.info("Services that support multiple ThriftService values:");
    printMultiServiceServers(zc, paths);
  }

  /**
   * Test that we can determine ServiceLockPath search parameters from ThriftService alone.
   */
  @Test
  public void testServiceLockPathFromThriftServiceOnly() {
    ServiceLockPaths paths = getServerContext().getServerPaths();

    Set<ServiceLockPath> managerPaths = paths.getForService(ThriftService.MANAGER,
        ResourceGroupPredicate.ANY, AddressSelector.all(), true);
    assertNotNull(managerPaths);
    assertFalse(managerPaths.isEmpty());

    Set<ServiceLockPath> gcPaths = paths.getForService(ThriftService.GC, ResourceGroupPredicate.ANY,
        AddressSelector.all(), true);
    assertNotNull(gcPaths);
    assertFalse(gcPaths.isEmpty());

    Set<ServiceLockPath> tservPaths = paths.getForService(ThriftService.TSERV,
        ResourceGroupPredicate.ANY, AddressSelector.all(), true);
    assertNotNull(tservPaths);
    assertFalse(tservPaths.isEmpty());

    Set<ServiceLockPath> clientPaths = paths.getForService(ThriftService.CLIENT,
        ResourceGroupPredicate.ANY, AddressSelector.all(), true);
    assertNotNull(clientPaths);
    assertFalse(clientPaths.isEmpty());

    Set<ServiceLockPath> coorPaths = paths.getForService(ThriftService.COORDINATOR,
        ResourceGroupPredicate.ANY, AddressSelector.all(), true);
    assertNotNull(coorPaths);
    assertFalse(coorPaths.isEmpty());

    Set<ServiceLockPath> fateWorkerPaths = paths.getForService(ThriftService.FATE_WORKER,
        ResourceGroupPredicate.ANY, AddressSelector.all(), true);
    assertNotNull(fateWorkerPaths);
    assertFalse(fateWorkerPaths.isEmpty());

    Set<ServiceLockPath> fateClientPaths = paths.getForService(ThriftService.FATE_CLIENT,
        ResourceGroupPredicate.ANY, AddressSelector.all(), true);
    assertNotNull(fateClientPaths);
    assertFalse(fateClientPaths.isEmpty());

    Set<ServiceLockPath> compactorPaths = paths.getForService(ThriftService.COMPACTOR,
        ResourceGroupPredicate.ANY, AddressSelector.all(), true);
    assertNotNull(compactorPaths);
    assertFalse(compactorPaths.isEmpty());

    Set<ServiceLockPath> tabletIngestPaths = paths.getForService(ThriftService.TABLET_INGEST,
        ResourceGroupPredicate.ANY, AddressSelector.all(), true);
    assertNotNull(tabletIngestPaths);
    assertFalse(tabletIngestPaths.isEmpty());

    Set<ServiceLockPath> tabletMgmtPaths = paths.getForService(ThriftService.TABLET_MANAGEMENT,
        ResourceGroupPredicate.ANY, AddressSelector.all(), true);
    assertNotNull(tabletMgmtPaths);
    assertFalse(tabletMgmtPaths.isEmpty());

    Set<ServiceLockPath> tabletScanPaths = paths.getForService(ThriftService.TABLET_SCAN,
        ResourceGroupPredicate.ANY, AddressSelector.all(), true);
    assertNotNull(tabletScanPaths);
    assertFalse(tabletScanPaths.isEmpty());

    Set<ServiceLockPath> noneScanPaths = paths.getForService(ThriftService.NONE,
        ResourceGroupPredicate.ANY, AddressSelector.all(), true);
    assertNotNull(noneScanPaths);
    assertTrue(noneScanPaths.isEmpty());
  }

  private void checkServiceLockForThriftServices(ZooCache zc, ServiceLockPath path,
      String serverType, Map<ThriftService,Set<String>> discoveredServices) {
    if (path == null) {
      return;
    }

    ZcStat stat = new ZcStat();
    Optional<ServiceLockData> lockData = ServiceLock.getLockData(zc, path, stat);

    ServiceLockData data = lockData.orElseThrow();
    // ServiceLockData contains ServiceDescriptors which list one or more ThriftServices
    // We need to check all possible ThriftService values to see which ones have data
    for (ThriftService service : ThriftService.values()) {
      if (data.getAddressString(service) != null) {
        discoveredServices.computeIfAbsent(service, k -> new HashSet<>()).add(serverType);
        log.debug("Found {} service in {} at path {}", service, serverType, path);
      }
    }
  }

  private void addMappingsFromLock(ZooCache zc, ServiceLockPath path,
      Map<ThriftService,Set<String>> serviceToPathType) {
    if (path == null) {
      return;
    }

    ZcStat stat = new ZcStat();
    Optional<ServiceLockData> lockData = ServiceLock.getLockData(zc, path, stat);

    ServiceLockData data = lockData.orElseThrow();
    String pathType = path.getType();

    for (ThriftService service : ThriftService.values()) {
      if (data.getAddressString(service) != null) {
        if (serviceToPathType.containsKey(service)) {
          Set<String> existingPath = serviceToPathType.get(service);
          existingPath.add(pathType);
        } else {
          var set = new HashSet<String>();
          set.add(pathType);
          serviceToPathType.put(service, set);
        }
      }
    }
  }

  private void validateMapping(Map<ThriftService,Set<String>> mapping, ThriftService service,
      Set<String> expectedPathTypes) {
    assertTrue(mapping.containsKey(service),
        "Mapping should contain " + service + " -> " + expectedPathTypes);
    assertEquals(expectedPathTypes, mapping.get(service),
        "Service " + service + " should map to " + expectedPathTypes);
  }

  private void printMultiServiceServers(ZooCache zc, ServiceLockPaths paths) {
    // Check which server types provide multiple ThriftServices
    checkMultiService(zc, paths.getManager(true), "Manager");
    checkMultiService(zc, paths.getGarbageCollector(true), "GC");
    checkMultiService(zc, paths.getGarbageCollector(true), "MONITOR");

    for (ServiceLockPath path : paths.getTabletServer(ResourceGroupPredicate.ANY,
        AddressSelector.all(), true)) {
      checkMultiService(zc, path, "TServer");
      break;
    }

    for (ServiceLockPath path : paths.getCompactor(ResourceGroupPredicate.ANY,
        AddressSelector.all(), true)) {
      checkMultiService(zc, path, "Compactor");
      break;
    }

    for (ServiceLockPath path : paths.getScanServer(ResourceGroupPredicate.ANY,
        AddressSelector.all(), true)) {
      checkMultiService(zc, path, "SServer");
      break;
    }
  }

  private void checkMultiService(ZooCache zc, ServiceLockPath path, String serverType) {
    if (path == null) {
      return;
    }

    ZcStat stat = new ZcStat();
    Optional<ServiceLockData> lockData = ServiceLock.getLockData(zc, path, stat);

    ServiceLockData data = lockData.orElseThrow();
    Set<ThriftService> services = EnumSet.noneOf(ThriftService.class);

    for (ThriftService service : ThriftService.values()) {
      if (data.getAddressString(service) != null) {
        services.add(service);
      }
    }

    if (services.size() > 1) {
      log.info("  {} provides {} services: {}", serverType, services.size(), services);
    }
  }
}
