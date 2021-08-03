package org.hkust.codegenerator;

import org.hkust.checkerutils.CheckerUtils;
import org.hkust.jsonutils.JsonParser;
import org.hkust.objects.*;
import org.hkust.schema.RelationSchema;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

public class CodeGenerator {
    public static final String GENERATED_CODE = "generated-code";
    private static final RelationSchema schema = new RelationSchema();


    public static void generate(String jsonFilePath, String jarOutputPath, String flinkInputPath, String flinkOutputPath, String[] dataSinkTypes) throws Exception {
        CheckerUtils.checkNullOrEmpty(jsonFilePath, "jsonFilePath");
        CheckerUtils.checkNullOrEmpty(jarOutputPath, "jarOutputPath");
        CheckerUtils.checkNullOrEmpty(flinkInputPath, "flinkInputPath");
        CheckerUtils.checkNullOrEmpty(flinkOutputPath, "flinkOutputPath");

        Node node = JsonParser.parse(jsonFilePath);

        String codeFilesPath = jarOutputPath + File.separator + GENERATED_CODE + File.separator + "src" + File.separator + "main" + File.separator + "scala" + File.separator + "org" + File.separator + "hkust";
        List<RelationProcessFunction> relationProcessFunctions = node.getRelationProcessFunctions();

        for (RelationProcessFunction relationProcessFunction : relationProcessFunctions) {
            new RelationProcessFunctionWriter(relationProcessFunction, schema).write(codeFilesPath);
        }

        List<AggregateProcessFunction> aggregateProcessFunctions = node.getAggregateProcessFunctions();
        for (AggregateProcessFunction aggregateProcessFunction : aggregateProcessFunctions) {
            if (aggregateProcessFunction.getClass() == DistinctCountProcessFunction.class) {
                new CountDistinctProcessFunctionWriter((DistinctCountProcessFunction) aggregateProcessFunction, schema).write(codeFilesPath);
            } else {
                new AggregateProcessFunctionWriter(aggregateProcessFunction, schema).write(codeFilesPath);
            }
        }

        TransformerFunction transformerFunction = node.getTransformerFunction();
        if (transformerFunction != null) {
            new TransformerFunctionWriter(transformerFunction, schema).write(codeFilesPath);
        }

        new MainClassWriter(node, schema, flinkInputPath, flinkOutputPath, dataSinkTypes).write(codeFilesPath);

        compile(jarOutputPath + File.separator + GENERATED_CODE + File.separator + "pom.xml");

    }

    private static void compile(String pomPath) throws IOException {
        Runtime runtime = Runtime.getRuntime();
        String mvnCommand = System.getProperty("os.name").toLowerCase().startsWith("windows") ? "mvn.cmd" : "mvn";
        execute(runtime, mvnCommand + " package -DskipTests -f " + pomPath);
    }

    private static void execute(Runtime runtime, String command) throws IOException {
        Process process = runtime.exec(command);

        BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));

        BufferedReader stdError = new BufferedReader(new InputStreamReader(process.getErrorStream()));

        System.out.println("Running " + command + ":\n");
        String output;
        while ((output = stdInput.readLine()) != null) {
            System.out.println(output);
        }

        System.out.println("Errors of " + command + " (if any):\n");
        while ((output = stdError.readLine()) != null) {
            System.out.println(output);
        }
    }
}
