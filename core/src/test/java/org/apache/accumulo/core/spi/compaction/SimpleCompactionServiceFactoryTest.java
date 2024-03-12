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

import java.util.HashMap;
import java.util.Map;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.util.ConfigurationImpl;
import org.easymock.EasyMock;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

public class SimpleCompactionServiceFactoryTest {

  @Test
  public void testSimpleImplementation() throws ReflectiveOperationException {
    Map<String,String> overrides = new HashMap<>();
    overrides.put(Property.COMPACTION_SERVICE_FACTORY.getKey(),
        Property.COMPACTION_SERVICE_FACTORY.getDefaultValue());
    overrides.put(Property.COMPACTION_SERVICE_FACTORY_CONFIG.getKey(),
        Property.COMPACTION_SERVICE_FACTORY_CONFIG.getDefaultValue());
    var conf = new ConfigurationImpl(SiteConfiguration.empty().withOverrides(overrides).build());
    var testCSF = new SimpleCompactionServiceFactory();

    ServiceEnvironment senv = EasyMock.createMock(ServiceEnvironment.class);
    EasyMock.expect(senv.getConfiguration()).andReturn(conf).anyTimes();
    EasyMock.expect(senv.getConfiguration(TableId.of("42"))).andReturn(conf).anyTimes();
    EasyMock
        .expect(
            senv.instantiate(RatioBasedCompactionPlanner.class.getName(), CompactionPlanner.class))
        .andReturn(new RatioBasedCompactionPlanner()).anyTimes();
    EasyMock.replay(senv);
    CompactionServiceFactory csf = null;
    assertEquals(testCSF.getClass().getName(),
        conf.get(Property.COMPACTION_SERVICE_FACTORY.getKey()));
    csf = testCSF;
    csf.init(senv);

    var planner = csf.forService(CompactionServiceId.of("default"));
    assertEquals(planner.getClass().getName(), RatioBasedCompactionPlanner.class.getName());

    planner = csf.forService(CompactionServiceId.of("Unknown"));
    assertEquals(planner.getClass().getName(), ProvisionalCompactionPlanner.class.getName());
  }

}
