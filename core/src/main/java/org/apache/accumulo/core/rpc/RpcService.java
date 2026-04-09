package org.apache.accumulo.core.rpc;

public enum RpcService {
  // Make this tiny for rpc length reduction
  CLIENT,
  COORDINATOR,
  COMPACTOR,
  FATE_CLIENT,
  FATE_WORKER,
  GC,
  MGR,
  NONE,
  TABLET_INGEST,
  TABLET_MGMT,
  TABLET_SCAN,
  TSERVER,
  SERVER_PROCESS,;
}
