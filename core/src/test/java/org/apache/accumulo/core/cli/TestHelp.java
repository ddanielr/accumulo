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
package org.apache.accumulo.core.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.beust.jcommander.Parameter;

public class TestHelp {
  protected class HelpStub extends Help {
    @Override
    public void parseArgs(String programName, String[] args, Object... others) {
      super.parseArgs(programName, args, others);
    }

    @Override
    public void exit(int status) {
      throw new IllegalStateException(Integer.toString(status));
    }
  }

  @Test
  public void testInvalidArgs() {
    String[] args = {"foo"};
    HelpStub help = new HelpStub();
    try {
      help.parseArgs("program", args);
    } catch (RuntimeException e) {
      assertEquals("1", e.getMessage());
    }
  }

  @Test
  public void testHelpCommand() {
    class TestHelpOpt extends HelpStub {
      @Parameter(names = {"--test"})
      boolean test = false;
    }

    String[] args = {"--help", "--test"};
    TestHelpOpt opts = new TestHelpOpt();
    try {
      opts.parseArgs("program", args);
      assertTrue(opts.test);
    } catch (RuntimeException e) {
      assertEquals("0", e.getMessage());
    }
  }
}
