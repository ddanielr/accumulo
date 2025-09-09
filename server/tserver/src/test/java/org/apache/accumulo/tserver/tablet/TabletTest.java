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
package org.apache.accumulo.tserver.tablet;

import static org.apache.accumulo.core.conf.Property.*;
import static org.apache.accumulo.core.conf.Property.TABLE_DURABILITY;
import static org.easymock.EasyMock.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.FileNotFoundException;
import java.time.Instant;
import java.util.*;

import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.NamespaceConfiguration;
import org.apache.accumulo.server.conf.SystemConfiguration;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.util.FileUtil;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.accumulo.tserver.TabletServerResourceManager.TabletResourceManager;
import org.apache.accumulo.tserver.compaction.CompactionPlan;
import org.apache.accumulo.tserver.compaction.WriteParameters;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

@SuppressWarnings("removal")
public class TabletTest {

  @Test
  public void correctValuesSetForProperties() {

    CompactionPlan plan = createMock(CompactionPlan.class);
    WriteParameters writeParams = createMock(WriteParameters.class);
    plan.writeParameters = writeParams;

    long hdfsBlockSize = 10000L, blockSize = 5000L, indexBlockSize = 500L;
    int replication = 5;
    String compressType = "snappy";

    expect(writeParams.getHdfsBlockSize()).andReturn(hdfsBlockSize).times(2);
    expect(writeParams.getBlockSize()).andReturn(blockSize).times(2);
    expect(writeParams.getIndexBlockSize()).andReturn(indexBlockSize).times(2);
    expect(writeParams.getCompressType()).andReturn(compressType).times(2);
    expect(writeParams.getReplication()).andReturn(replication).times(2);

    EasyMock.replay(plan, writeParams);

    Map<String,String> aConf = CompactableUtils.computeOverrides(writeParams);

    EasyMock.verify(plan, writeParams);

    assertEquals(hdfsBlockSize, Long.parseLong(aConf.get(Property.TABLE_FILE_BLOCK_SIZE.getKey())));
    assertEquals(blockSize,
        Long.parseLong(aConf.get(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey())));
    assertEquals(indexBlockSize,
        Long.parseLong(aConf.get(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX.getKey())));
    assertEquals(compressType, aConf.get(Property.TABLE_FILE_COMPRESSION_TYPE.getKey()));
    assertEquals(replication,
        Integer.parseInt(aConf.get(Property.TABLE_FILE_REPLICATION.getKey())));
  }

  @Test
  public void testCompactionDuringSplitComputation() throws Exception {


    final TableId TID = TableId.of("3");
    final NamespaceId NID = NamespaceId.of("2");

    InstanceId instanceId = InstanceId.of(UUID.randomUUID());
    ServerContext context = createMock(ServerContext.class);
    expect(context.getInstanceID()).andReturn(instanceId).anyTimes();
    PropStore propStore = createMock(ZooPropStore.class);
    expect(context.getPropStore()).andReturn(propStore).anyTimes();
    var siteConfig = SiteConfiguration.empty().build();
    expect(context.getSiteConfiguration()).andReturn(siteConfig).anyTimes();

    replay(context);


    // this test is ignoring listeners
    propStore.registerAsListener(anyObject(), anyObject());
    expectLastCall().anyTimes();

    var sysPropKey = SystemPropKey.of(instanceId);
    VersionedProperties sysProps =
            new VersionedProperties(1, Instant.now(), Map.of(TABLE_BLOOM_ENABLED.getKey(), "true"));
    expect(propStore.get(eq(sysPropKey))).andReturn(sysProps).times(2);

    var nsPropKey = NamespacePropKey.of(instanceId, NID);
    VersionedProperties nsProps = new VersionedProperties(2, Instant.now(),
            Map.of(TABLE_FILE_MAX.getKey(), "21", TABLE_BLOOM_ENABLED.getKey(), "false"));
    expect(propStore.get(eq(nsPropKey))).andReturn(nsProps).once();

    var tablePropKey = TablePropKey.of(instanceId, TID);
    VersionedProperties tableProps =
            new VersionedProperties(3, Instant.now(), Map.of(TABLE_BLOOM_ENABLED.getKey(), "true"));
    expect(propStore.get(eq(tablePropKey))).andReturn(tableProps).once();

    ConfigurationCopy defaultConfig =
            new ConfigurationCopy(Map.of(TABLE_BLOOM_SIZE.getKey(), TABLE_BLOOM_SIZE.getDefaultValue(),
                    TABLE_DURABILITY.getKey(), TABLE_DURABILITY.getDefaultValue()));

    replay(propStore);

    SystemConfiguration sysConfig = new SystemConfiguration(context, sysPropKey, defaultConfig);
    NamespaceConfiguration nsConfig = new NamespaceConfiguration(context, nsPropKey.getId(), sysConfig);
    TableConfiguration tableConfig = new TableConfiguration(context, tablePropKey.getId(), nsConfig);
    expect(context.getTableConfiguration(anyObject())).andReturn(tableConfig).atLeastOnce();

    TabletServer tabletServer = createMock(TabletServer.class);
    TabletResourceManager tabletResourceManager = createMock(TabletResourceManager.class);
    VolumeManager volumeManager = createMock(VolumeManager.class);

    FileSystem fileSystem = createMock(FileSystem.class);

    DatafileManager datafileManager = createMock(DatafileManager.class);
    ServiceLock serviceLock = createMock(ServiceLock.class);

    expect(volumeManager.getFileSystemByPath(anyObject(Path.class))).andReturn(fileSystem).anyTimes();

    AccumuloConfiguration accumuloConfiguration = createMock(AccumuloConfiguration.class);
    expect(accumuloConfiguration.getCount(anyObject(Property.class))).andReturn(10).atLeastOnce();
    expect(context.getConfiguration()).andReturn(accumuloConfiguration).atLeastOnce();
    expect(context.getVolumeManager()).andReturn(volumeManager).atLeastOnce();
    expect(context.getVolumeReplacements()).andReturn(List.of()).atLeastOnce();

    expect(tabletServer.getContext()).andReturn(context).atLeastOnce();
    expect(tabletServer.createLogId()).andReturn(1).atLeastOnce();
    expect(tabletServer.getLock()).andReturn(serviceLock).anyTimes();
    expect(tabletServer.getVolumeManager()).andReturn(volumeManager).atLeastOnce();

    KeyExtent extent = new KeyExtent(TID, new Text("abcd"), new Text("abc"));
    TabletFile testFile1 = new TabletFile(new Path("hdfs://namenode:9020/accumulo/tables/1/default_tablet/F00001.rf"));
    TabletFile testFile2 = new TabletFile(new Path("hdfs://namenode:9020/accumulo/tables/1/default_tablet/F00002.rf"));
    TabletFile testFile3 = new TabletFile(new Path("hdfs://namenode:9020/accumulo/tables/1/default_tablet/F00003.rf"));
    HashSet<TabletFile> files = new HashSet<>();
    files.add(testFile1);
    files.add(testFile2);
    files.add(testFile3);

    SortedMap<StoredTabletFile, DataFileValue> sortedMap = new TreeMap<>();

    expect(datafileManager.getFiles()).andReturn(files).atLeastOnce();
    expect(datafileManager.getDatafileSizes()).andReturn(sortedMap).atLeastOnce();
    TServerInstance tServerInstance = new TServerInstance("localhost:1234[SESSION]");


    TabletData tabletData = new TabletData("testDir", sortedMap,
            new MetadataTime(1234L, TimeType.MILLIS), 1L, 1L, Location.current(tServerInstance),
            Collections.emptyMap());

    replay(tabletServer, context, tableConfig);

    // Create partial mock
    Tablet tablet = createMockBuilder(Tablet.class).withConstructor(TabletServer.class, KeyExtent.class, TabletResourceManager.class,
            TabletData.class)
            .withArgs(tabletServer, extent, tabletResourceManager, tabletData)
            .addMockedMethod("getDatafileManager")
            .addMockedMethod("chooseTabletDir")
            .addMockedMethod("isSplitPossible")
            .addMockedMethod("isClosing")
            .addMockedMethod("isClosed")
            .createMock();

    expect(tablet.getDatafileManager()).andReturn(datafileManager).anyTimes();
    expect(tablet.chooseTabletDir()).andReturn("/test/tablet/dir").anyTimes();
    expect(tablet.isSplitPossible()).andReturn(true).anyTimes();

    expect(tablet.isClosing()).andReturn(false).anyTimes();
    expect(tablet.isClosed()).andReturn(false).anyTimes();

      anyObject(Boolean.class);
      expect(FileUtil.findMidPoint(anyObject(), anyObject(), anyString(), anyObject(), anyObject(), anyObject(),
              anyDouble(), anyBoolean())).andThrow(new FileNotFoundException("File does not exist")).anyTimes();

      expect(FileUtil.toPathStrings(anyObject())).andReturn(Collections.singletonList("hdfs://namenode:9020/accumulo/tables/1/default_tablet/F00001.rf")).anyTimes();

      replay(tabletServer, tabletResourceManager, context, volumeManager, fileSystem, accumuloConfiguration,
              tableConfig, datafileManager, tablet);

    Optional<Tablet.SplitComputations> result = tablet.getSplitComputations();

    assertFalse(result.isPresent(), "getSplitComputations should return empty Optional when FileNotFoundException is caught");

    verify(tabletServer, tabletResourceManager, context, volumeManager, fileSystem, accumuloConfiguration,
            tableConfig, datafileManager, tablet);
  }
}
