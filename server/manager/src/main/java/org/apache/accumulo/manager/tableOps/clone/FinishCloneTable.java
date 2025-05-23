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
package org.apache.accumulo.manager.tableOps.clone;

import java.util.EnumSet;

import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.zookeeper.DistributedReadWriteLock.LockType;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.slf4j.LoggerFactory;

class FinishCloneTable extends ManagerRepo {

  private static final long serialVersionUID = 1L;
  private final CloneInfo cloneInfo;

  public FinishCloneTable(CloneInfo cloneInfo) {
    this.cloneInfo = cloneInfo;
  }

  @Override
  public long isReady(FateId fateId, Manager environment) {
    return 0;
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager environment) {
    // directories are intentionally not created.... this is done because directories should be
    // unique
    // because they occupy a different namespace than normal tablet directories... also some clones
    // may never create files.. therefore there is no need to consume namenode space w/ directories
    // that are not used... tablet will create directories as needed

    final EnumSet<TableState> expectedCurrStates = EnumSet.of(TableState.NEW);
    if (cloneInfo.isKeepOffline()) {
      environment.getTableManager().transitionTableState(cloneInfo.getTableId(), TableState.OFFLINE,
          expectedCurrStates);
    } else {
      // transition clone table state to state of original table
      TableState ts = environment.getTableManager().getTableState(cloneInfo.getSrcTableId());
      environment.getTableManager().transitionTableState(cloneInfo.getTableId(), ts,
          expectedCurrStates);
    }

    Utils.unreserveNamespace(environment, cloneInfo.getSrcNamespaceId(), fateId, LockType.READ);
    if (!cloneInfo.getSrcNamespaceId().equals(cloneInfo.getNamespaceId())) {
      Utils.unreserveNamespace(environment, cloneInfo.getNamespaceId(), fateId, LockType.READ);
    }
    Utils.unreserveTable(environment, cloneInfo.getSrcTableId(), fateId, LockType.READ);
    Utils.unreserveTable(environment, cloneInfo.getTableId(), fateId, LockType.WRITE);

    environment.getEventCoordinator().event(cloneInfo.getTableId(), "Cloned table %s from %s",
        cloneInfo.getTableName(), cloneInfo.getSrcTableId());

    LoggerFactory.getLogger(FinishCloneTable.class)
        .debug("Cloned table " + cloneInfo.getSrcTableId() + " " + cloneInfo.getTableId() + " "
            + cloneInfo.getTableName());

    return null;
  }

  @Override
  public void undo(FateId fateId, Manager environment) {}

}
