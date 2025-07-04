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
package org.apache.accumulo.tserver.logger;

import static org.apache.accumulo.tserver.logger.LogEvents.COMPACTION_FINISH;
import static org.apache.accumulo.tserver.logger.LogEvents.COMPACTION_START;
import static org.apache.accumulo.tserver.logger.LogEvents.DEFINE_TABLET;
import static org.apache.accumulo.tserver.logger.LogEvents.MANY_MUTATIONS;
import static org.apache.accumulo.tserver.logger.LogEvents.MUTATION;
import static org.apache.accumulo.tserver.logger.LogEvents.OPEN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.server.data.ServerMutation;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class LogFileTest {

  private static void readWrite(LogEvents event, long seq, int tid, String filename,
      KeyExtent tablet, Mutation[] mutations, LogFileKey keyResult, LogFileValue valueResult)
      throws IOException {
    LogFileKey key = new LogFileKey();
    key.setEvent(event);
    key.setSeq(seq);
    key.setTabletId(tid);
    key.setFilename(filename);
    key.setTablet(tablet);
    key.setTserverSession(keyResult.getTserverSession());
    LogFileValue value = new LogFileValue();
    value.setMutations(Arrays.asList(mutations != null ? mutations : new Mutation[0]));
    DataOutputBuffer out = new DataOutputBuffer();
    key.write(out);
    value.write(out);
    out.flush();
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.size());
    keyResult.readFields(in);
    valueResult.readFields(in);
    assertEquals(0, key.compareTo(keyResult));
    assertEquals(value.getMutations(), valueResult.getMutations());
    assertEquals(in.read(), -1);
  }

  @Test
  public void testReadFields() throws IOException {
    LogFileKey key = new LogFileKey();
    LogFileValue value = new LogFileValue();
    key.setTserverSession("");
    readWrite(OPEN, -1, -1, null, null, null, key, value);
    assertEquals(key.getEvent(), OPEN);
    readWrite(COMPACTION_FINISH, 1, 2, null, null, null, key, value);
    assertEquals(key.getEvent(), COMPACTION_FINISH);
    assertEquals(key.getSeq(), 1);
    assertEquals(key.getTabletId(), 2);
    readWrite(COMPACTION_START, 3, 4, "some file", null, null, key, value);
    assertEquals(key.getEvent(), COMPACTION_START);
    assertEquals(key.getSeq(), 3);
    assertEquals(key.getTabletId(), 4);
    assertEquals(key.getFilename(), "some file");
    KeyExtent tablet = new KeyExtent(TableId.of("table"), new Text("bbbb"), new Text("aaaa"));
    readWrite(DEFINE_TABLET, 5, 6, null, tablet, null, key, value);
    assertEquals(key.getEvent(), DEFINE_TABLET);
    assertEquals(key.getSeq(), 5);
    assertEquals(key.getTabletId(), 6);
    assertEquals(key.getTablet(), tablet);
    Mutation m = new ServerMutation(new Text("row"));
    m.put("cf", "cq", "value");
    readWrite(MUTATION, 7, 8, null, null, new Mutation[] {m}, key, value);
    assertEquals(key.getEvent(), MUTATION);
    assertEquals(key.getSeq(), 7);
    assertEquals(key.getTabletId(), 8);
    assertEquals(value.getMutations(), Arrays.asList(m));
    m = new ServerMutation(new Text("row"));
    m.put(new Text("cf"), new Text("cq"), new ColumnVisibility("vis"), 12345, new Value("value"));
    m.put(new Text("cf"), new Text("cq"), new ColumnVisibility("vis2"), new Value("value"));
    m.putDelete(new Text("cf"), new Text("cq"), new ColumnVisibility("vis2"));
    readWrite(MUTATION, 8, 9, null, null, new Mutation[] {m}, key, value);
    assertEquals(key.getEvent(), MUTATION);
    assertEquals(key.getSeq(), 8);
    assertEquals(key.getTabletId(), 9);
    assertEquals(value.getMutations(), Arrays.asList(m));
    readWrite(MANY_MUTATIONS, 9, 10, null, null, new Mutation[] {m, m}, key, value);
    assertEquals(key.getEvent(), MANY_MUTATIONS);
    assertEquals(key.getSeq(), 9);
    assertEquals(key.getTabletId(), 10);
    assertEquals(value.getMutations(), Arrays.asList(m, m));
  }

  @Test
  public void testEventType() {
    assertEquals(LogFileKey.eventType(MUTATION), LogFileKey.eventType(MANY_MUTATIONS));
    assertEquals(LogFileKey.eventType(COMPACTION_START), LogFileKey.eventType(COMPACTION_FINISH));
    assertTrue(LogFileKey.eventType(DEFINE_TABLET) < LogFileKey.eventType(COMPACTION_FINISH));
    assertTrue(LogFileKey.eventType(COMPACTION_FINISH) < LogFileKey.eventType(MUTATION));

  }

}
