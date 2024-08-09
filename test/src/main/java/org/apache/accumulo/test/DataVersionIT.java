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
package org.apache.accumulo.test;

import java.io.File;
import java.util.LinkedList;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.ProcessReference;
import org.apache.accumulo.server.AccumuloDataVersion;
import org.apache.accumulo.start.Main;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jline.utils.Log;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

public class DataVersionIT extends SharedMiniClusterBase {

  private static Logger LOG = LoggerFactory.getLogger(DataVersionIT.class);

  @BeforeAll
  public static void setup() throws Exception {
    startMiniCluster();
  }

  @AfterAll
  public static void tearDown() {
    stopMiniCluster();
  }

  @Test
  public void test() throws Exception {

    File root = getCluster().getConfig().getAccumuloDir();
    String path = root + "/" + Constants.VERSION_DIR;
    Path rootPath = new Path(path + "/" + AccumuloDataVersion.get());
    FileSystem fs = getCluster().getFileSystem();
    Path newPath = new Path(path + "/" + (AccumuloDataVersion.get() + 1));

    assertTrue(fs.exists(rootPath));

    getCluster().stop();
    LOG.info("Stopped cluster");

    getCluster().getClusterControl().start(ServerType.ZOOKEEPER);
    Log.info("Started ZooKeeper");


    LOG.info("Incrementing Data Version");
    fs.create(newPath, true);
    fs.delete(rootPath, false);

    for (ServerType st : ServerType.values()) {

      if (!st.prettyPrint().equals("Master") && !st.prettyPrint().equals("Tracer") && !st.prettyPrint().equals("ZooKeeper")) {
        var serverClass = st.prettyPrint();
        LOG.info("Attempting to start process name: {}", serverClass);

        getCluster().getClusterControl().start(st);
        var processes = getCluster().getProcesses().get(st).stream().map(ProcessReference::getProcess).collect(
            Collectors.toList());
        for (Process p : processes) {
          p.waitFor();
          int exitCode = p.exitValue();

          // Check to see if both paths exist at the same time
          assertFalse(fs.exists(rootPath));
          assertTrue(fs.exists(newPath));
          assertNotEquals(0, exitCode, "Expected non-zero exit code for server type "
              + st.prettyPrint() + ", but got " + exitCode);
        }
      }
    }
  }
}
