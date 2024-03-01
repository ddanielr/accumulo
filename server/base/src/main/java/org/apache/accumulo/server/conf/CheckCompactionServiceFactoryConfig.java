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
package org.apache.accumulo.server.conf;

import java.io.FileNotFoundException;
import java.nio.file.Path;

import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.compaction.CompactionServiceFactory;
import org.apache.accumulo.core.util.ConfigurationImpl;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

/**
 * A command line tool that verifies that a given properties file will correctly configure
 * compaction services.
 *
 * This tool takes, as input, a local path to a properties file containing the properties used to
 * configure compaction services. The file is parsed and the user is presented with output detailing
 * which (if any) compaction services would be created from the given properties, or an error
 * describing why the given properties are incorrect.
 */
@AutoService(KeywordExecutable.class)
public class CheckCompactionServiceFactoryConfig implements KeywordExecutable {

  private final static Logger log =
      LoggerFactory.getLogger(CheckCompactionServiceFactoryConfig.class);

  static class Opts extends Help {
    @Parameter(
        description = "<path> Local path to file containing compaction factory configuration",
        required = true)
    String filePath;
  }

  @Override
  public String keyword() {
    return "check-compaction-service-factory-config";
  }

  @Override
  public String description() {
    return "Verifies compaction service factory config within a given file";
  }

  public static void main(String[] args) throws Exception {
    new CheckCompactionServiceFactoryConfig().execute(args);
  }

  @Override
  public void execute(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(keyword(), args);

    if (opts.filePath == null) {
      throw new IllegalArgumentException("No properties file was given");
    }

    Path path = Path.of(opts.filePath);
    if (!path.toFile().exists()) {
      throw new FileNotFoundException("File at given path could not be found");
    }

    AccumuloConfiguration config = SiteConfiguration.fromFile(path.toFile()).build();
    ServiceEnvironment senv = createServiceEnvironment(config);
    var factoryClassName = config.get(Property.COMPACTION_SERVICE_FACTORY);

    Class<? extends CompactionServiceFactory> factoryClass =
        Class.forName(factoryClassName).asSubclass(CompactionServiceFactory.class);
    CompactionServiceFactory factory = factoryClass.getDeclaredConstructor().newInstance();

    factory.init(senv);

    log.info("Properties file has passed all checks.");
  }

  private ServiceEnvironment createServiceEnvironment(AccumuloConfiguration config) {
    return new ServiceEnvironment() {

      @Override
      public <T> T instantiate(TableId tableId, String className, Class<T> base) {
        throw new UnsupportedOperationException();
      }

      @Override
      public <T> T instantiate(String className, Class<T> base) {
        throw new UnsupportedOperationException();
      }

      @Override
      public String getTableName(TableId tableId) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Configuration getConfiguration(TableId tableId) {
        return new ConfigurationImpl(config);
      }

      @Override
      public Configuration getConfiguration() {
        return new ConfigurationImpl(config);
      }
    };
  }
}
