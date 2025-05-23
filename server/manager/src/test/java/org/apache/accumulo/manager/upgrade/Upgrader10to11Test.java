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
package org.apache.accumulo.manager.upgrade;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.Constants.ZTABLE_STATE;
import static org.apache.accumulo.manager.upgrade.Upgrader10to11.buildRepTablePath;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Upgrader10to11Test {
  private static final Logger log = LoggerFactory.getLogger(Upgrader10to11Test.class);

  private InstanceId instanceId;
  private ServerContext context;
  private ZooSession zk;
  private PropStore propStore;

  @BeforeEach
  public void initMocks() {
    instanceId = InstanceId.of(UUID.randomUUID());
    context = createMock(ServerContext.class);
    zk = createMock(ZooSession.class);
    propStore = createMock(PropStore.class);

    expect(context.getZooSession()).andReturn(zk).anyTimes();
    expect(zk.asReaderWriter()).andReturn(new ZooReaderWriter(zk)).anyTimes();
    expect(context.getInstanceID()).andReturn(instanceId).anyTimes();
  }

  @AfterEach
  public void verifyMocks() {
    verify(context, zk, propStore);
  }

  @Test
  void upgradeZooKeeperGoPath() throws Exception {
    expect(context.getPropStore()).andReturn(propStore).anyTimes();
    expect(zk.exists(buildRepTablePath(instanceId), null)).andReturn(new Stat()).once();
    expect(zk.getData(buildRepTablePath(instanceId) + ZTABLE_STATE, null, null))
        .andReturn(TableState.OFFLINE.name().getBytes(UTF_8)).once();
    expect(zk.getChildren(buildRepTablePath(instanceId), null)).andReturn(List.of());
    zk.delete(buildRepTablePath(instanceId), -1);
    expectLastCall().once();

    expect(propStore.get(TablePropKey.of(SystemTables.METADATA.tableId())))
        .andReturn(new VersionedProperties()).once();

    replay(context, zk, propStore);

    Upgrader10to11 upgrader = new Upgrader10to11();
    upgrader.upgradeZookeeper(context);
  }

  @Test
  void upgradeZookeeperNoReplTableNode() throws Exception {
    expect(zk.exists(buildRepTablePath(instanceId), null)).andReturn(null).once();
    replay(context, zk, propStore);

    Upgrader10to11 upgrader = new Upgrader10to11();
    upgrader.upgradeZookeeper(context);
  }

  @Test
  void checkReplicationStateOffline() throws Exception {

    expect(context.getPropStore()).andReturn(propStore).anyTimes();
    expect(zk.exists(buildRepTablePath(instanceId), null)).andReturn(new Stat()).once();
    expect(zk.getData(buildRepTablePath(instanceId) + ZTABLE_STATE, null, null))
        .andReturn(TableState.OFFLINE.name().getBytes(UTF_8)).once();
    expect(zk.getChildren(buildRepTablePath(instanceId), null)).andReturn(List.of());
    zk.delete(buildRepTablePath(instanceId), -1);
    expectLastCall().once();
    expect(propStore.get(TablePropKey.of(SystemTables.METADATA.tableId())))
        .andReturn(new VersionedProperties()).once();

    replay(context, zk, propStore);

    Upgrader10to11 upgrader = new Upgrader10to11();

    upgrader.upgradeZookeeper(context);
  }

  @Test
  void checkReplicationStateOnline() throws Exception {
    expect(zk.exists(buildRepTablePath(instanceId), null)).andReturn(new Stat()).once();
    expect(zk.getData(buildRepTablePath(instanceId) + ZTABLE_STATE, null, null))
        .andReturn(TableState.ONLINE.name().getBytes(UTF_8)).anyTimes();
    replay(context, zk, propStore);

    Upgrader10to11 upgrader = new Upgrader10to11();
    assertThrows(IllegalStateException.class, () -> upgrader.upgradeZookeeper(context));
  }

  @Test
  void checkReplicationStateNoNode() throws Exception {
    expect(zk.exists(buildRepTablePath(instanceId), null)).andReturn(new Stat()).once();
    expect(zk.getData(buildRepTablePath(instanceId) + ZTABLE_STATE, null, null))
        .andThrow(new KeeperException.NoNodeException("force no node exception")).anyTimes();
    replay(context, zk, propStore);

    Upgrader10to11 upgrader = new Upgrader10to11();
    assertThrows(IllegalStateException.class, () -> upgrader.upgradeZookeeper(context));
  }

  @Test
  public void filterTest() {
    Map<String,String> entries = new HashMap<>();
    entries.put("table.file.compress.blocksize", "32K");
    entries.put("table.file.replication", "5");
    entries.put("table.group.server", "file,log,srv,future");
    entries.put("table.iterator.majc.bulkLoadFilter",
        "20,org.apache.accumulo.server.iterators.MetadataBulkLoadFilter");
    entries.put("table.iterator.majc.replcombiner",
        "9,org.apache.accumulo.server.replication.StatusCombiner");
    entries.put("table.iterator.majc.replcombiner.opt.columns", "stat");
    entries.put("table.iterator.majc.vers",
        "10,org.apache.accumulo.core.iterators.user.VersioningIterator");
    entries.put("table.iterator.majc.vers.opt.maxVersions", "1");
    entries.put("table.iterator.minc.replcombiner",
        "9,org.apache.accumulo.server.replication.StatusCombiner");
    entries.put("table.iterator.minc.replcombiner.opt.columns", "stat");
    entries.put("table.iterator.minc.vers",
        "10,org.apache.accumulo.core.iterators.user.VersioningIterator");
    entries.put("table.iterator.minc.vers.opt.maxVersions", "1");
    entries.put("table.iterator.scan.replcombiner",
        "9,org.apache.accumulo.server.replication.StatusCombiner");
    entries.put("table.iterator.scan.replcombiner.opt.columns", "stat");
    entries.put("table.iterator.scan.vers",
        "10,org.apache.accumulo.core.iterators.user.VersioningIterator");

    String REPL_ITERATOR_PATTERN = "^table\\.iterator\\.(majc|minc|scan)\\.replcombiner$";
    String REPL_COLUMN_PATTERN =
        "^table\\.iterator\\.(majc|minc|scan)\\.replcombiner\\.opt\\.columns$";

    Pattern p = Pattern.compile("(" + REPL_ITERATOR_PATTERN + "|" + REPL_COLUMN_PATTERN + ")");

    List<String> filtered =
        entries.keySet().stream().filter(e -> p.matcher(e).find()).collect(Collectors.toList());

    assertEquals(6, filtered.size());
    log.info("F:{}", filtered);

    replay(context, zk, propStore);
  }
}
