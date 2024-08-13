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
package org.apache.accumulo.core.compaction;

import static org.apache.accumulo.core.compaction.CompactionServiceFactoryLoader.ClassloaderType.ACCUMULO;
import static org.apache.accumulo.core.compaction.CompactionServiceFactoryLoader.ClassloaderType.JAVA;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner;
import org.apache.accumulo.core.spi.compaction.CompactionServiceFactory;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.core.spi.compaction.NoCompactionServiceFactory;
import org.apache.accumulo.core.spi.compaction.SimpleCompactionServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactionServiceFactoryLoader {
  private static final Logger log = LoggerFactory
      .getLogger(org.apache.accumulo.core.compaction.CompactionServiceFactoryLoader.class);

  // Initialize Default state for compaction factory
  private static final CompactionServiceFactory NO_COMPACTION_FACTORY =
      new NoCompactionServiceFactory();

  enum ClassloaderType {
    // Use the Accumulo custom classloader. Should only be used by Accumulo server side code.
    ACCUMULO,
    // Use basic Java classloading mechanism. Should be use by Accumulo client code.
    JAVA
  }

  /**
   * Creates a new server Factory.
   */
  public static CompactionServiceFactory newInstance(PluginEnvironment env) {
    String clazzName = env.getConfiguration().get(Property.COMPACTION_SERVICE_FACTORY.getKey());
    return loadCompactionServiceFactory(ACCUMULO, clazzName);
  }

  /**
   * For use by server utilities not associated with a table. Requires Instance, general and table
   * configuration. Creates a new Factory from the configuration and gets the CompactionPlanner from
   * that Factory.
   */
  public static CompactionPlanner getServiceForServer(CompactionServiceId csid,
      PluginEnvironment env) {
    CompactionServiceFactory factory = newInstance(env);
    return factory.getPlanner(null, csid, env);
  }

  private static CompactionServiceFactory loadCompactionServiceFactory(ClassloaderType ct,
      String clazzName) {
    log.debug("Creating new compaction factory class {}", clazzName);
    CompactionServiceFactory newCompactionServiceFactory;

    if (ct == ACCUMULO) {
      newCompactionServiceFactory = ConfigurationTypeHelper.getClassInstance(null, clazzName,
          CompactionServiceFactory.class, new SimpleCompactionServiceFactory());
    } else if (ct == JAVA) {
      if (clazzName == null || clazzName.trim().isEmpty()) {
        newCompactionServiceFactory = NO_COMPACTION_FACTORY;
      } else {
        try {
          newCompactionServiceFactory = CompactionServiceFactoryLoader.class.getClassLoader()
              .loadClass(clazzName).asSubclass(CompactionServiceFactory.class)
              .getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
          throw new RuntimeException(e);
        }
      }
    } else {
      throw new IllegalArgumentException();
    }
    return newCompactionServiceFactory;
  }
}
