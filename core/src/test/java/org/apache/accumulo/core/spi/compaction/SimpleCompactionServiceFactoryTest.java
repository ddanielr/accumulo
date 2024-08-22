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

  @Test
  public void testSimpleImplementation() throws ReflectiveOperationException {
    Map<String,String> overrides = new HashMap<>();
    overrides.put(COMPACTION_SERVICE_FACTORY.getKey(),
        COMPACTION_SERVICE_FACTORY.getDefaultValue());
    overrides.put(COMPACTION_SERVICE_FACTORY_CONFIG.getKey(), "{ \"default\": {\"planner\": \""
        + RatioBasedCompactionPlanner.class.getName()
        + "\", \"opts\": {\"maxOpenFilesPerJob\": \"30\"}, \"groups\": [{\"group\": \""
        + DEFAULT_RESOURCE_GROUP_NAME + "\", \"maxSize\": \"128M\"}]},"
        + "\"csf1\" : {\"planner\": \"" + RatioBasedCompactionPlanner.class.getName()
        + "\", \"opts\": {\"maxOpenFilesPerJob\": \"30\"}, \"groups\": [{\"group\": \"DCQ1\"}]},"
        + "\"csf2\" : {\"planner\": \"" + RatioBasedCompactionPlanner.class.getName()
        + "\", \"opts\": {\"maxOpenFilesPerJob\": \"30\"}, \"groups\": [{\"group\": \"DCQ2\"}]},"
        + "\"csf3\" : {\"planner\": \"" + RatioBasedCompactionPlanner.class.getName()
        + "\", \"opts\": {\"maxOpenFilesPerJob\": \"30\"}, \"groups\": [{\"group\": \"DCQ3\"}]},"
        + "\"csf4\" : {\"planner\": \"" + RatioBasedCompactionPlanner.class.getName()
        + "\", \"opts\": {\"maxOpenFilesPerJob\": \"30\"}, \"groups\": [{\"group\": \"DCQ4\"}]},"
        + "\"csf5\" : {\"planner\": \"" + RatioBasedCompactionPlanner.class.getName()
        + "\", \"opts\": {\"maxOpenFilesPerJob\": \"30\"}, \"groups\": [{\"group\": \"DCQ5\"}]},"
        + "\"csf6\" : {\"planner\": \"" + RatioBasedCompactionPlanner.class.getName()
        + "\", \"opts\": {\"maxOpenFilesPerJob\": \"30\"}, \"groups\": [{\"group\": \"DCQ6\"}]},"
        + "\"csf7\" : {\"planner\": \"" + RatioBasedCompactionPlanner.class.getName()
        + "\", \"opts\": {\"maxOpenFilesPerJob\": \"30\"}, \"groups\": [{\"group\": \"DCQ7\"}]},"
        + "\"csf8\" : { \"planner\": \"" + RatioBasedCompactionPlanner.class.getName()
        + "\", \"opts\": {\"maxOpenFilesPerJob\": \"30\"}, \"groups\": [{\"group\": \"DCQ8\"}]}}");
    var conf = new ConfigurationImpl(SiteConfiguration.empty().withOverrides(overrides).build());
    var testCSF = new SimpleCompactionServiceFactory();

    var tableId = TableId.of("42");

    PluginEnvironment env = EasyMock.createMock(PluginEnvironment.class);
    EasyMock.expect(env.getConfiguration()).andReturn(conf).anyTimes();
    EasyMock.expect(env.getConfiguration(tableId)).andReturn(conf).anyTimes();
    EasyMock.expect(env.instantiate(tableId, RatioBasedCompactionPlanner.class.getName(),
        CompactionPlanner.class)).andReturn(new RatioBasedCompactionPlanner()).anyTimes();
    EasyMock.replay(env);
    CompactionServiceFactory csf;
    assertEquals(testCSF.getClass().getName(), conf.get(COMPACTION_SERVICE_FACTORY.getKey()));
    csf = testCSF;
    csf.init(env);

    // Replace these with the planner makePlan tests from any PlannerTests
    /*
     * var planner = csf.getPlanner(tableId, CompactionServiceId.of("default"), env);
     * assertEquals(planner.getClass().getName(), RatioBasedCompactionPlanner.class.getName());
     *
     * planner = csf.getPlanner(tableId, CompactionServiceId.of("Unknown"), env);
     * assertEquals(planner.getClass().getName(), ProvisionalCompactionPlanner.class.getName());
     *
     * planner = csf.getPlanner(tableId, CompactionServiceId.of("csf1"), env);
     * assertEquals(planner.getClass().getName(), RatioBasedCompactionPlanner.class.getName());
     */
  }

}
