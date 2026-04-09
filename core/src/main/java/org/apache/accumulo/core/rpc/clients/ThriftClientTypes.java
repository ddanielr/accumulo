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
package org.apache.accumulo.core.rpc.clients;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService;
import org.apache.accumulo.core.compaction.thrift.CompactorService;
import org.apache.accumulo.core.gc.thrift.GCMonitorService;
import org.apache.accumulo.core.manager.thrift.FateWorkerService;
import org.apache.accumulo.core.rpc.RpcService;
import org.apache.accumulo.core.tabletingest.thrift.TabletIngestClientService;
import org.apache.accumulo.core.tabletscan.thrift.TabletScanClientService;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;

public class ThriftClientTypes<C extends TServiceClient> {

  public static final ClientServiceThriftClient CLIENT =
      new ClientServiceThriftClient(RpcService.CLIENT);

  public static final ThriftClientTypes<CompactorService.Client> COMPACTOR =
      new ThriftClientTypes<>(RpcService.COMPACTOR, new CompactorService.Client.Factory());

  public static final ThriftClientTypes<CompactionCoordinatorService.Client> COORDINATOR =
      new ThriftClientTypes<>(RpcService.COORDINATOR,
          new CompactionCoordinatorService.Client.Factory());

  public static final FateThriftClient FATE_CLIENT = new FateThriftClient(RpcService.FATE_CLIENT);

  public static final ThriftClientTypes<GCMonitorService.Client> GC =
      new ThriftClientTypes<>(RpcService.GC, new GCMonitorService.Client.Factory());

  public static final ManagerThriftClient MANAGER = new ManagerThriftClient(RpcService.MGR);

  public static final TabletServerThriftClient TABLET_SERVER =
      new TabletServerThriftClient(RpcService.TSERVER);

  public static final ThriftClientTypes<TabletScanClientService.Client> TABLET_SCAN =
      new ThriftClientTypes<>(RpcService.TABLET_SCAN, new TabletScanClientService.Client.Factory());

  public static final ThriftClientTypes<TabletIngestClientService.Client> TABLET_INGEST =
      new ThriftClientTypes<>(RpcService.TABLET_INGEST,
          new TabletIngestClientService.Client.Factory());

  public static final TabletManagementClientServiceThriftClient TABLET_MGMT =
      new TabletManagementClientServiceThriftClient(RpcService.TABLET_MGMT);

  public static final ServerProcessServiceThriftClient SERVER_PROCESS =
      new ServerProcessServiceThriftClient(RpcService.SERVER_PROCESS);

  public static final ThriftClientTypes<FateWorkerService.Client> FATE_WORKER =
      new ThriftClientTypes<>(RpcService.FATE_WORKER, new FateWorkerService.Client.Factory());

  /**
   * execute method with supplied client returning object of type R
   *
   * @param <R> return type
   * @param <C> client type
   */
  public interface Exec<R,C> {
    R execute(C client) throws TException;
  }

  /**
   * execute method with supplied client
   *
   * @param <C> client type
   */
  public interface ExecVoid<C> {
    void execute(C client) throws TException;
  }

  private final RpcService serviceName;
  private final TServiceClientFactory<C> clientFactory;;

  protected ThriftClientTypes(RpcService serviceName, TServiceClientFactory<C> factory) {
    this.serviceName = serviceName;
    this.clientFactory = factory;
  }

  public final String getServiceName() {
    return serviceName.name();
  }

  public final TServiceClientFactory<C> getClientFactory() {
    return clientFactory;
  }

  public C getClient(TProtocol prot) {
    // All server side TProcessors are multiplexed. Wrap this protocol.
    return getClientFactory().getClient(new TMultiplexedProtocol(prot, getServiceName()));
  }

  public C getConnection(ClientContext context) {
    throw new UnsupportedOperationException("This method has not been implemented");
  }

  public C getConnectionWithRetry(ClientContext context) {
    while (true) {
      C result = getConnection(context);
      if (result != null) {
        return result;
      }
      sleepUninterruptibly(250, MILLISECONDS);
    }
  }

  public <R> R execute(ClientContext context, Exec<R,C> exec)
      throws AccumuloException, AccumuloSecurityException {
    throw new UnsupportedOperationException("This method has not been implemented");
  }

  public void executeVoid(ClientContext context, ExecVoid<C> exec)
      throws AccumuloException, AccumuloSecurityException {
    throw new UnsupportedOperationException("This method has not been implemented");
  }

  @Override
  public String toString() {
    return getServiceName();
  }
}
