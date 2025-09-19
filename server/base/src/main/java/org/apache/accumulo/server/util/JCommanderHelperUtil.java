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
package org.apache.accumulo.server.util;

import org.apache.accumulo.server.util.annotation.CommandName;

import com.beust.jcommander.JCommander;

public class JCommanderHelperUtil {
  /**
   * Creates a JCommander instance and automatically sets the program name based on the CommandName
   * annotation present on the command object's class.
   *
   * @param commandObject the object containing JCommander annotations
   * @return a configured JCommander instance
   * @throws IllegalArgumentException if the command object's class is not annotated
   *         with @CommandName
   */
  public static JCommander createWithProgramName(Object commandObject) {
    Class<?> clazz = commandObject.getClass();
    CommandName commandNameAnnotation = clazz.getAnnotation(CommandName.class);

    if (commandNameAnnotation == null) {
      throw new IllegalArgumentException(
          "Class " + clazz.getSimpleName() + " must be annotated with @CommandName");
    }

    JCommander jc = new JCommander(commandObject);
    jc.setProgramName(commandNameAnnotation.value());
    return jc;
  }

  /**
   * Creates a JCommander instance with multiple command objects and automatically sets the program
   * name from the main command object.
   *
   * @param mainCommandObject the main command object (must have @CommandName annotation)
   * @param additionalObjects additional command objects to add to JCommander
   * @return a configured JCommander instance
   * @throws IllegalArgumentException if the main command object's class is not annotated
   *         with @CommandName
   */
  public static JCommander createWithProgramName(Object mainCommandObject,
      Object... additionalObjects) {
    JCommander jc = createWithProgramName(mainCommandObject);

    // Add any additional command objects
    for (Object obj : additionalObjects) {
      jc.addObject(obj);
    }

    return jc;
  }

  /**
   * Creates a JCommander instance without a main object and sets the program name based on the
   * CommandName annotation of the specified class.
   *
   * @param commandClass the class containing the CommandName annotation
   * @return a configured JCommander instance
   * @throws IllegalArgumentException if the class is not annotated with @CommandName
   */
  public static JCommander createWithProgramName(Class<?> commandClass) {
    CommandName commandNameAnnotation = commandClass.getAnnotation(CommandName.class);

    if (commandNameAnnotation == null) {
      throw new IllegalArgumentException(
          "Class " + commandClass.getSimpleName() + " must be annotated with @CommandName");
    }

    JCommander jc = new JCommander();
    jc.setProgramName(commandNameAnnotation.value());
    return jc;
  }
}
