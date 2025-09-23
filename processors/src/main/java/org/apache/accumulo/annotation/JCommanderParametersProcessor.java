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
package org.apache.accumulo.annotation;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.google.auto.service.AutoService;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.StandardLocation;

@AutoService(Processor.class)
@SupportedAnnotationTypes({ "com.beust.jcommander.Parameter", "com.beust.jcommander.Parameters",
        "com.beust.jcommander.ParametersDelegate" })
@SupportedSourceVersion(SourceVersion.RELEASE_11)
public class JCommanderParametersProcessor extends AbstractProcessor {

    private final Map<String, CommandInfo> commands = new LinkedHashMap<>();
    private final String outputFile = "_accumulo_completions";

    private static class CommandInfo {
        final String name;
        final String description;
        final String className;
        final List<ParameterInfo> parameters = new ArrayList<>();

        CommandInfo(String name, String description, String className) {
            this.name = name;
            this.description = description;
            this.className = className;
        }
    }

    private static class ParameterInfo {
        final String name;
        final String description;
        final boolean required;
        final String defaultValue;
        final String type;

        ParameterInfo(String name, String description, boolean required, String defaultValue, String type) {
            this.name = name;
            this.description = description;
            this.required = required;
            this.defaultValue = defaultValue;
            this.type = type;
        }
    }

    @Override
    public boolean process(Set<? extends TypeElement> set, RoundEnvironment roundEnvironment) {

        if (roundEnvironment.processingOver()) {
            try {
                FileObject file;
                file = processingEnv.getFiler().createResource(StandardLocation.SOURCE_OUTPUT, "", outputFile);

                try (PrintWriter writer = new PrintWriter(file.openWriter())) {
                    writeScript(writer);
                }

                processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE,
                        "Generated bash autocompletion script: " + file.getName());

            } catch (IOException e) {
                processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                        "Failed to generate bash autocompletion script: " + e.getMessage());
            }
            return true;
        }

        for (Element element : roundEnvironment.getElementsAnnotatedWith(Parameters.class)) {
            if (element.getKind().equals(ElementKind.CLASS)) {
                processParametersClass((TypeElement) element);
            }
        }

        return true;
    }

    private void processParametersClass(TypeElement element) {
        Parameters parametersAnnotation = element.getAnnotation(Parameters.class);
        if (parametersAnnotation == null) {
            return;
        }

        String className = element.getQualifiedName().toString();
        String[] commandNames = parametersAnnotation.commandNames();
        String description = parametersAnnotation.commandDescription();

        if (commandNames.length == 0) {
            String simpleName = element.getSimpleName().toString();
            commandNames = new String[] { simpleName.toLowerCase(Locale.ROOT) };
        }

        for (String commandName : commandNames) {
            CommandInfo commandInfo = commands.computeIfAbsent(commandName,
                    c -> new CommandInfo(c, description, className));

            processParameterFields(element, commandInfo);
            processDelegateFields(element, commandInfo);
        }
    }

    private void processParameterFields(TypeElement element, CommandInfo commandInfo) {
        for (Element enclosedElement : element.getEnclosedElements()) {
            if (enclosedElement.getKind() == ElementKind.FIELD) {
                VariableElement field = (VariableElement) enclosedElement;
                Parameter parameterAnnotation = field.getAnnotation(Parameter.class);

                if (parameterAnnotation != null) {
                    String[] names = parameterAnnotation.names();
                    String description = parameterAnnotation.description();
                    boolean required = parameterAnnotation.required();
                    // Check if the parameter is a flag or if it expects a value
                    String defaultValue = parameterAnnotation.arity() > 0 ? "" : "false";

                    for (String name : names) {
                        ParameterInfo paramInfo = new ParameterInfo(name, description, required, defaultValue,
                                field.asType().toString());
                        commandInfo.parameters.add(paramInfo);
                    }
                }
            }
        }

        TypeMirror superclass = element.getSuperclass();
        if (superclass != null) {
            Element superElement = processingEnv.getTypeUtils().asElement(superclass);
            if (superElement instanceof TypeElement) {
                processParameterFields((TypeElement) superElement, commandInfo);
            }
        }
    }

    private void processDelegateFields(TypeElement classElement, CommandInfo commandInfo) {
        for (Element enclosedElement : classElement.getEnclosedElements()) {
            if (enclosedElement.getKind() == ElementKind.FIELD) {
                VariableElement field = (VariableElement) enclosedElement;
                ParametersDelegate delegateAnnotation = field.getAnnotation(ParametersDelegate.class);

                if (delegateAnnotation != null) {
                    // Process the delegate class
                    TypeMirror fieldType = field.asType();
                    Element fieldTypeElement = processingEnv.getTypeUtils().asElement(fieldType);
                    if (fieldTypeElement instanceof TypeElement) {
                        processParameterFields((TypeElement) fieldTypeElement, commandInfo);
                    }
                }
            }
        }
    }

    private void writeScript(PrintWriter writer) {

        // Write header information
        writer.println("#! /usr/bin/env bash");
        writer.println("# Auto-generated bash autocompletion script for accumulo");
        writer.println("# Generated by: " + getClass().getName());
        writer.println();

        // Generate completion functions for each command
        for (CommandInfo command : commands.values()) {
            writeCommand(writer, command);
        }

        writer.println("# Main completion dispatcher");
        writer.println("_jcommander_main_completion() {");
        writer.println("    local command=\"$1\"");
        writer.println("    case \"$command\" in");

        for (String commandName : commands.keySet()) {
            writer.printf("        %s)%n", commandName);
            writer.printf("            _%s_completion%n", commandName.replace("-", "_"));
            writer.println("            ;;");
        }

        writer.println("    esac");
        writer.println("}");
        writer.println();

        // Register completion functions
        for (String commandName : commands.keySet()) {
            writer.printf("complete -F _%s_completion %s%n", commandName.replace("-", "_"), commandName);
        }
        writer.println();
    }

    private void writeCommand(PrintWriter writer, CommandInfo command) {
        writer.printf("# Completion for command: %s%n", command.name);
        writer.printf("# Description: %s%n", command.description);
        writer.printf("_%s_completion() {%n", command.name.replace("-", "_"));
        writer.println("    local cur prev opts");
        writer.println("    COMPREPLY=()");
        writer.println("    cur=\"${COMP_WORDS[COMP_CWORD]}\"");
        writer.println("    prev=\"${COMP_WORDS[COMP_CWORD-1]}\"");
        writer.println();

        // Generate options list
        Set<String> allOptions = command.parameters.stream().map(p -> p.name).collect(Collectors.toSet());

        writer.print("    opts=\"");
        writer.print(String.join(" ", allOptions));
        writer.println("\"");
        writer.println();

        // Generate parameter-specific completions
        for (ParameterInfo param : command.parameters) {
            if (param.name.startsWith("--") && !param.type.equals("boolean")) {
                writer.printf("    if [[ ${prev} == \"%s\" ]]; then%n", param.name);
                generateParameterCompletion(writer, param);
                writer.println("        return 0");
                writer.println("    fi");
                writer.println();
            }
        }

        // Default completion
        writer.println("    if [[ ${cur} == -* ]]; then");
        writer.println("        COMPREPLY=( $(compgen -W \"${opts}\" -- ${cur}) )");
        writer.println("        return 0");
        writer.println("    fi");
        writer.println("}");
        writer.println();
    }

    private void generateParameterCompletion(PrintWriter writer, ParameterInfo param) {
        String paramArgs="";
        String pType = param.type;
        String description = param.description.toLowerCase(Locale.ROOT);

        // Generate type-specific completions
        if (pType.contains("File") || pType.contains("Path")) {
            paramArgs="-f";
        } else if (pType.contains("Directory")) {
            paramArgs="-d";
        } else if (pType.equals("boolean")) {
            paramArgs="-W \"true false\"";
        } else if (description.contains("host")
                || description.contains("server")) {
            paramArgs="-A hostname";
        }
        if (paramArgs.isEmpty()) {
            writer.printf("        # %s - %s%n", param.description, param.type);
            writer.println("        COMPREPLY=()");
        } else {
            writer.printf("        COMPREPLY=( $(compgen %s -- \"${cur}\") )\n", paramArgs);
        }
    }
}
