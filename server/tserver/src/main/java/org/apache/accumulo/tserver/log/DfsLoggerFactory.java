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
package org.apache.accumulo.tserver.log;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.crypto.CryptoEnvironmentImpl;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.spi.crypto.CryptoServiceFactory;
import org.apache.accumulo.core.spi.wal.WriteAheadLog;
import org.apache.accumulo.core.spi.wal.WriteAheadLogFactory;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.server.ServiceEnvironmentImpl;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.wal.ServerWalInitParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DfsLoggerFactory implements WriteAheadLogFactory {

  private static final Logger log = LoggerFactory.getLogger(DfsLoggerFactory.class);

  private VolumeManager volumeManager;
  private AccumuloConfiguration conf;
  private CryptoServiceFactory cryptoServiceFactory;
  private Set<String> baseUris;
  private AtomicLong syncCounter;
  private AtomicLong flushCounter;
  private DfsWalReader walReader;

  @Override
  public void init(InitParameters params) {
    if (!(params instanceof ServerWalInitParameters serverWalInitParameters)) {
      throw new IllegalArgumentException(
          "DfsLoggerFactory requires ServerWalInitParameters but received: "
              + params.getClass().getName()
              + ". Ensure the server passes a ServerWalInitParameters implementation");
    }

    this.volumeManager = serverWalInitParameters.getVolumeManager();
    this.baseUris = serverWalInitParameters.getBaseUris();
    ServiceEnvironmentImpl senv =
        (ServiceEnvironmentImpl) serverWalInitParameters.getServiceEnvironment();
    this.conf = senv.getContext().getConfiguration();
    this.cryptoServiceFactory = senv.getContext().getCryptoFactory();

    this.syncCounter = serverWalInitParameters.getSyncCounter();
    this.flushCounter = serverWalInitParameters.getFlushCounter();

    CryptoEnvironment env = new CryptoEnvironmentImpl(CryptoEnvironment.Scope.RECOVERY);
    CryptoService readerCryptoService =
        cryptoServiceFactory.getService(env, conf.getAllCryptoProperties());
    this.walReader = new DfsWalReader(readerCryptoService, conf);

    log.info("DfsLoggerFactory initialized");
  }

  @Override
  public WriteAheadLog createNew(String address) throws IOException {
    Objects.requireNonNull(walReader,
        "DfsLoggerFactory.init() must be called before createNew() or getReader()");
    return DfsLogger.createNew(volumeManager, conf, cryptoServiceFactory, baseUris, syncCounter,
        flushCounter, address);
  }

  @Override
  public WriteAheadLog fromLogEntry(LogEntry logEntry) {
    return DfsLogger.fromLogEntry(logEntry);
  }

  @Override
  public WalReader getReader() {
    Objects.requireNonNull(walReader,
        "DfsLoggerFactory.init() must be called before createNew() or getReader()");
    return walReader;
  }

}
