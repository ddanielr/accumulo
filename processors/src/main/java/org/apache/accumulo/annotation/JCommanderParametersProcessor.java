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
    private String filePrefix = "";

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
            generateBashCompletionScript();
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

        // Do not recalculate the file prefix once its set.
        if (filePrefix.isEmpty()) {
            if (element.getEnclosingElement().getSimpleName().toString().contains("Admin")) {
                filePrefix = "accumulo-admin";
            } else if (className.contains("org.apache.accumulo.shell")) {
                filePrefix = "accumulo-shell";
            }
        }

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
                    processParameterAnnotation(field, parameterAnnotation, commandInfo);
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

    private void processParameterAnnotation(VariableElement field, Parameter annotation, CommandInfo commandInfo) {
        String[] names = annotation.names();
        String description = annotation.description();
        boolean required = annotation.required();
        String defaultValue = annotation.arity() > 0 ? "" : "false"; // Flags vs parameters

        for (String name : names) {
            ParameterInfo paramInfo = new ParameterInfo(name, description, required, defaultValue,
                    field.asType().toString());
            commandInfo.parameters.add(paramInfo);
        }
    }

    private void generateBashCompletionScript() {
        try {
            if (!filePrefix.isEmpty()) {
                filePrefix = filePrefix + "-";

            }
            String outputFile = filePrefix + "autocomplete.sh";

            FileObject file;
            file = processingEnv.getFiler().createResource(StandardLocation.SOURCE_OUTPUT, "", outputFile);

            try (PrintWriter writer = new PrintWriter(file.openWriter())) {
                generateBashScript(writer);
            }

            processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE,
                    "Generated bash completion script: " + file.getName());

        } catch (IOException e) {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                    "Failed to generate bash completion script: " + e.getMessage());
        }
    }

    private void generateBashScript(PrintWriter writer) {
        writer.println("#!/bin/bash");
        writer.println("# Auto-generated bash completion script for JCommander parameters");
        writer.println("# Generated by: " + getClass().getName());
        writer.println();

        // Generate completion functions for each command
        for (CommandInfo command : commands.values()) {
            generateCommandCompletion(writer, command);
        }

        // Generate main completion dispatcher
        generateMainCompletion(writer);

        // Generate data export functions
        generateDataExportFunctions(writer);
    }

    private void generateCommandCompletion(PrintWriter writer, CommandInfo command) {
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
        // Generate type-specific completions
        if (param.type.contains("File") || param.type.contains("Path")) {
            writer.println("        COMPREPLY=( $(compgen -f -- \"${cur}\") )");
        } else if (param.type.contains("Directory")) {
            writer.println("        COMPREPLY=( $(compgen -d -- \"${cur}\") )");
        } else if (param.type.equals("boolean")) {
            writer.println("        COMPREPLY=( $(compgen -W \"true false\" -- \"${cur}\") )");
        } else if (param.description.toLowerCase().contains("host")
                || param.description.toLowerCase().contains("server")) {
            writer.println("        COMPREPLY=( $(compgen -A hostname -- \"${cur}\") )");
        } else {
            writer.printf("        # %s - %s%n", param.description, param.type);
            writer.println("        COMPREPLY=()");
        }
    }

    private void generateMainCompletion(PrintWriter writer) {
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

    private void generateDataExportFunctions(PrintWriter writer) {
        writer.println("# Data export functions for external processing");
        writer.println();

        writer.println("# Export all command information as JSON");
        writer.println("jcommander_export_json() {");
        writer.println("    cat << 'EOF'");
        writer.println("{");
        writer.println("  \"commands\": [");

        boolean first = true;
        for (CommandInfo command : commands.values()) {
            if (!first)
                writer.println(",");
            first = false;

            writer.println("    {");
            writer.printf("      \"name\": \"%s\",%n", escapeJson(command.name));
            writer.printf("      \"description\": \"%s\",%n", escapeJson(command.description));
            writer.printf("      \"className\": \"%s\",%n", escapeJson(command.className));
            writer.println("      \"parameters\": [");

            boolean firstParam = true;
            for (ParameterInfo param : command.parameters) {
                if (!firstParam)
                    writer.println(",");
                firstParam = false;

                writer.println("        {");
                writer.printf("          \"name\": \"%s\",%n", escapeJson(param.name));
                writer.printf("          \"description\": \"%s\",%n", escapeJson(param.description));
                writer.printf("          \"required\": %s,%n", param.required);
                writer.printf("          \"type\": \"%s\"%n", escapeJson(param.type));
                writer.print("        }");
            }

            writer.println();
            writer.println("      ]");
            writer.print("    }");
        }

        writer.println();
        writer.println("  ]");
        writer.println("}");
        writer.println("EOF");
        writer.println("}");
        writer.println();

        // Export function for specific command
        writer.println("# Export specific command parameters");
        writer.println("jcommander_export_command_params() {");
        writer.println("    local command=\"$1\"");
        writer.println("    case \"$command\" in");

        for (CommandInfo command : commands.values()) {
            writer.printf("        %s)%n", command.name);
            writer.print("            echo \"");
            writer.print(command.parameters.stream().map(p -> p.name).collect(Collectors.joining(" ")));
            writer.println("\"");
            writer.println("            ;;");
        }

        writer.println("        *)");
        writer.println("            echo \"Unknown command: $command\" >&2");
        writer.println("            return 1");
        writer.println("            ;;");
        writer.println("    esac");
        writer.println("}");
    }

    private String escapeJson(String str) {
        return str.replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
    }
}
