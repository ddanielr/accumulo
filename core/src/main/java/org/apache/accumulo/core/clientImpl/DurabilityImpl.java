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
package org.apache.accumulo.core.clientImpl;

import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.tabletingest.thrift.TDurability;

public class DurabilityImpl {

  public static TDurability toThrift(Durability durability) {
    return switch (durability) {
      case DEFAULT -> TDurability.DEFAULT;
      case NONE -> TDurability.NONE;
      case LOG -> TDurability.LOG;
      case FLUSH -> TDurability.FLUSH;
      case BATCH_SYNC -> TDurability.BATCH_SYNC;
      case SYNC -> TDurability.SYNC;
    };
  }

  public static Durability fromString(String value) {
    return Durability.valueOf(value.toUpperCase());
  }

  public static Durability fromThrift(TDurability tdurabilty) {
    if (tdurabilty == null) {
      return Durability.DEFAULT;
    }
    return switch (tdurabilty) {
      case NONE -> Durability.NONE;
      case LOG -> Durability.LOG;
      case FLUSH -> Durability.FLUSH;
      case BATCH_SYNC -> Durability.BATCH_SYNC;
      case SYNC -> Durability.SYNC;
      case DEFAULT -> Durability.DEFAULT;
    };
  }

  public static Durability resolveDurability(Durability durability, Durability tabletDurability) {
    if (durability == Durability.DEFAULT) {
      return tabletDurability;
    }
    return durability;
  }

}
