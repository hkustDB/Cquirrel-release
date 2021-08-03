package org.hkust.codegenerator;

import org.ainslec.picocog.PicoWriter;
import org.hkust.checkerutils.CheckerUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

interface ClassWriter {
    String write(final String filePath) throws Exception;
    void addImports(final PicoWriter writer);

    void addConstructorAndOpenClass(final PicoWriter writer);

    default void closeClass(final PicoWriter writer) {
        writer.writeln_r("}");
    }

    default void writeClassFile(final String className, final String path, final String code) throws IOException {
        CheckerUtils.checkNullOrEmpty(className, "className");
        CheckerUtils.checkNullOrEmpty(path, "path");
        CheckerUtils.checkNullOrEmpty(code, "code");
        Files.write(Paths.get(path + File.separator + className + ".scala"), code.getBytes());
    }

    default String getProcessFunctionClassName(String name) {
        CheckerUtils.checkNullOrEmpty(name, "name");
        return name + "ProcessFunction";
    }
}
