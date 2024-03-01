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
package org.apache.accumulo.core.util.compaction;

import static org.apache.accumulo.core.conf.Property.COMPACTION_SERVICE_FACTORY_CONFIG;

import java.util.Objects;
import java.util.function.Predicate;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;

/**
 * This class serves to configure a compaction service factory from an {@link AccumuloConfiguration}
 * object. Specifically, the compaction service factory config property
 * {@link Property#COMPACTION_SERVICE_FACTORY_CONFIG} is used.
 */
public class CompactionServiceFactoryConfig {
  private final String config;

  public CompactionServiceFactoryConfig(AccumuloConfiguration aconf) {
    this(aconf.get(COMPACTION_SERVICE_FACTORY_CONFIG), aconf::isPropertySet);
  }

  public CompactionServiceFactoryConfig(PluginEnvironment.Configuration conf) {
    this(conf.get(COMPACTION_SERVICE_FACTORY_CONFIG.getKey()),
        property -> conf.isSet(property.getKey()));
  }

  private CompactionServiceFactoryConfig(String factoryConfig, Predicate<Property> isSetPredicate) {
    // Throws exception if the factory configuration isn't set, but the factory has been set.
    if (isSetPredicate.test(COMPACTION_SERVICE_FACTORY_CONFIG)
        && !isSetPredicate.test(Property.COMPACTION_SERVICE_FACTORY)) {
      throw new IllegalStateException(
          "Compaction Service Factory Config must also be defined if property "
              + Property.COMPACTION_SERVICE_FACTORY.getKey() + " is used");
    }
    this.config = factoryConfig;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof CompactionServiceFactoryConfig) {
      var oc = (CompactionServiceFactoryConfig) o;
      return getConfig().equals(oc.getConfig());
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(config);
  }

  public String getConfig() {
    return config;
  }
}
