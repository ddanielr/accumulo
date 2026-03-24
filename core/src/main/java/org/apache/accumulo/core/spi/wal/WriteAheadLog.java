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

import java.io.Closeable;
import java.io.IOException;
import java.io.Serial;
import java.util.Collection;
import java.util.List;

import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.tabletserver.log.LogEntry;

/**
 * Defines a potential abstraction interface for WALs. All write operations return a
 * {@link Operation} That callers must await to confirm durability.
 *
 * @since 4.0.0
 */
public interface WriteAheadLog extends Closeable {

  /* Static Identifier */
  LogEntry getLogEntry();

  // Number of serialized write operations (size-based rotations)
  long getWrites();

  // Write operations
  WriteAheadLog.Operation defineTablet(long seq, int tabletId, KeyExtent extent) throws IOException;

  WriteAheadLog.Operation log(long seq, int tabletId, Mutation m, Durability d) throws IOException;

  WriteAheadLog.Operation logManyTablets(Collection<TabletWrite> writes) throws IOException;

  WriteAheadLog.Operation minorCompactionStarted(long seq, int tabletId,
      String fullQualifiedFileName, Durability durability) throws IOException;

  WriteAheadLog.Operation minorCompactionFinished(long seq, int tabletId, Durability durability)
      throws IOException;

  // Async result of a WAL write operation -- allows callers to block until durable
  interface Operation {
    void await() throws IOException;
  }

  /**
   * Groups mutations for a single tablet destined for a single WAL write. This is the SPI safe
   * replacement for the internal {@code TabletMutations} type.
   */

  interface TabletWrite {
    long getSequence();

    int getTabletId();

    List<Mutation> getMutations();

    Durability getDurability();
  }

  class LogClosedException extends IOException {
    @Serial
    private static final long serialVersionUID = 1L;

    public LogClosedException() {
      super("LogClosed");
    }
  }
}
