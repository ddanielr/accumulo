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

import static org.apache.accumulo.core.Constants.DEFAULT_RESOURCE_GROUP_NAME;
import static org.apache.accumulo.core.conf.Property.COMPACTION_SERVICE_FACTORY;
import static org.apache.accumulo.core.conf.Property.COMPACTION_SERVICE_FACTORY_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.util.ConfigurationImpl;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

public class SimpleCompactionServiceFactoryTest {
  public static final String GROUP1 = "DCQ1";
  public static final String GROUP2 = "DCQ2";
  public static final String GROUP3 = "DCQ3";
  public static final String GROUP4 = "DCQ4";
  public static final String GROUP5 = "DCQ5";
  public static final String GROUP6 = "DCQ6";
  public static final String GROUP7 = "DCQ7";
  public static final String GROUP8 = "DCQ8";

  @Test
  public void testSimpleImplementation() throws ReflectiveOperationException {
    Map<String,String> overrides = new HashMap<>();
    overrides.put(COMPACTION_SERVICE_FACTORY.getKey(),
        COMPACTION_SERVICE_FACTORY.getDefaultValue());
    overrides.put(COMPACTION_SERVICE_FACTORY_CONFIG.getKey(),
        "{ \"default\": { \"maxOpenFilesPerJob\": \"30\", \"groups\": [{ \"group\": \""
            + DEFAULT_RESOURCE_GROUP_NAME + "\", \"maxSize\": \"128M\"}]},"
            + "\"csf1\" : { \"maxOpenFilesPerJob\": \"30\", \"groups\": [{ \"group\": \"" + GROUP1
            + "\"}]}, \"csf2\" : { \"maxOpenFilesPerJob\": \"30\", \"groups\": [{ \"group\": \""
            + GROUP2
            + "\"}]}, \"csf3\" : { \"maxOpenFilesPerJob\": \"30\", \"groups\": [{ \"group\": \""
            + GROUP3
            + "\"}]}, \"csf4\" : { \"maxOpenFilesPerJob\": \"30\", \"groups\": [{ \"group\": \""
            + GROUP4
            + "\"}]}, \"csf5\" : { \"maxOpenFilesPerJob\": \"30\", \"groups\": [{ \"group\": \""
            + GROUP5
            + "\"}]}, \"csf6\" : { \"maxOpenFilesPerJob\": \"30\", \"groups\": [{ \"group\": \""
            + GROUP6
            + "\"}]}, \"csf7\" : { \"maxOpenFilesPerJob\": \"30\", \"groups\": [{ \"group\": \""
            + GROUP7
            + "\"}]}, \"csf8\" : { \"maxOpenFilesPerJob\": \"30\", \"groups\": [{ \"group\": \""
            + GROUP8 + "\"}]}}");
    var conf = new ConfigurationImpl(SiteConfiguration.empty().withOverrides(overrides).build());
    var testCSF = new SimpleCompactionServiceFactory();

    var tableId = TableId.of("42");

    PluginEnvironment env = EasyMock.createMock(PluginEnvironment.class);
    EasyMock.expect(env.getConfiguration()).andReturn(conf).anyTimes();
    EasyMock.expect(env.getConfiguration(tableId)).andReturn(conf).anyTimes();
    EasyMock.expect(env.instantiate(tableId, RatioBasedCompactionPlanner.class.getName(),
        CompactionPlanner.class)).andReturn(new RatioBasedCompactionPlanner()).anyTimes();
    EasyMock.replay(env);
    CompactionServiceFactory csf = null;
    assertEquals(testCSF.getClass().getName(), conf.get(COMPACTION_SERVICE_FACTORY.getKey()));
    csf = testCSF;
    csf.init(env);

    var planner = csf.getPlanner(tableId, CompactionServiceId.of("default"));
    assertEquals(planner.getClass().getName(), RatioBasedCompactionPlanner.class.getName());

    planner = csf.getPlanner(tableId, CompactionServiceId.of("Unknown"));
    assertEquals(planner.getClass().getName(), ProvisionalCompactionPlanner.class.getName());

    planner = csf.getPlanner(tableId, CompactionServiceId.of("csf1"));
    assertEquals(planner.getClass().getName(), RatioBasedCompactionPlanner.class.getName());
  }

}
