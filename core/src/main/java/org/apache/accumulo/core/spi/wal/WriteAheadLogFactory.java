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
package org.apache.accumulo.core.spi.wal;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Set;

import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.tabletserver.log.LogEntry;

/**
 * A Factory that returns a WriteAheadLog implementation based on the environment and configuration.
 *
 * @since 4.0.0
 */
public interface WriteAheadLogFactory {

  interface InitParameters {
    ServiceEnvironment getServiceEnvironment();

    Set<String> getBaseUris();
  }

  /**
   * Provides a format aware reader. This should contain the crypto interface
   */

  interface WalReader {

    DataInputStream open(DataInputStream raw) throws IOException, WalHeaderIncompleteException;

    long getWalBlockSize();
  }

  /**
   * A well-timed tabletserver failure could result in an incomplete header written to a write-ahead
   * log. This exception is thrown when the header cannot be read from a WAL which should only
   * happen when the tserver dies as described.
   */
  class WalHeaderIncompleteException extends Exception {
    private static final long serialVersionUID = 1L;

    public WalHeaderIncompleteException(EOFException cause) {
      super("WAL header is incomplete. The tablet server likely died during header write", cause);
    }
  }

  void init(InitParameters params);

  WriteAheadLog createNew(String address) throws IOException;

  WriteAheadLog fromLogEntry(LogEntry logEntry);

  WalReader getReader();
}
