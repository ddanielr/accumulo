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
package org.apache.accumulo.tserver.compaction;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Set;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.spi.compaction.CompactionServiceFactory;
import org.apache.accumulo.core.spi.compaction.SimpleCompactionServiceFactory;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CompactionServiceFactoryTest {

  private ServerContext context;

  @BeforeEach
  public void init() throws IOException {
    ConfigurationCopy config = new ConfigurationCopy(DefaultConfiguration.getInstance());
    CompactionServiceFactory compactionServiceFactory = new SimpleCompactionServiceFactory();

    config.set(Property.INSTANCE_VOLUMES.getKey(), "file:///");

    context = mock(ServerContext.class);
    expect(context.getCompactionServiceFactory()).andReturn(compactionServiceFactory).anyTimes();
    expect(context.getConfiguration()).andReturn(config).anyTimes();
    expect(context.getHadoopConf()).andReturn(new Configuration()).anyTimes();
    VolumeManager volumeManager = VolumeManagerImpl.get(config, new Configuration());
    expect(context.getVolumeManager()).andReturn(volumeManager).anyTimes();
    replay(context);
  }

  @AfterEach
  public void verifyMock() {
    verify(context);
  }

  /**
   * Validate that the compactionServiceFactory can actually be called from the serverContext
   */
  @Test
  public void getCompactionServiceFactoryTest() throws Exception {

    CompactionServiceFactory compactionServiceFactory = context.getCompactionServiceFactory();
    assertEquals(compactionServiceFactory.getCompactionServiceIds(), Set.of());

  }

}
