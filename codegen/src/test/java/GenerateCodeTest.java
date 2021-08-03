import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;

import static java.io.File.separator;

public class GenerateCodeTest {
    private static final String QUERY_FOLDER_PREFIX = "q";
    private static final String QUERY_JSON_FILE_PREFIX = "Q";
    private static final Integer QUERY_FOLDER_PREFIX_LEN = 1;
    private static final String JSON_FILE_SUFFIX = ".json";
    private static final String GENERATED_CODE = "generated-code";
    private static final String FLINK_INPUT_FILE_PATH = "file:///aju/q3flinkInput.csv";
    private static final String FLINK_OUTPUT_FILE_PATH = "file:///aju/q3flinkOutput.csv";
    private static final ArrayList<Integer> queryArrayList = new ArrayList<>();
    private static final String RESOURCE_FOLDER = new File("src" + separator + "test" + separator + "resources").getAbsolutePath();
    private final String CODEGEN_JAR_PATH = new File(CodeGen.class.getProtectionDomain().getCodeSource().getLocation().getPath()).getParentFile().getAbsolutePath()
            + separator + "codegen-1.0-SNAPSHOT.jar";

    @BeforeAll
    static void getQueryArrayList() throws Exception {
        removeEveryGeneratedCodeFolder();
        File resourceFolderFile = new File(RESOURCE_FOLDER);
        if (!resourceFolderFile.exists() || !resourceFolderFile.isDirectory()) {
            throw new FileNotFoundException("resource folder does not exists.");
        }

        File[] resourceFolderFiles = resourceFolderFile.listFiles();
        for (File file : resourceFolderFiles) {
            if (!validateQueryFolderExist(file)) {
                continue;
            }
            int queryIdx;
            try {
                queryIdx = Integer.parseInt(file.getName().substring(QUERY_FOLDER_PREFIX_LEN));
            } catch (NumberFormatException e) {
                System.out.println(file.getName() + " is not a query name.");
                continue;
            }
            if (validateQueryJsonFileExist(queryIdx)) {
                queryArrayList.add(queryIdx);
            }
        }
    }

    @Test
    public void generateCodeForEachQuery() throws Exception {
        for (int queryIdx : queryArrayList) {
            generateCodeForGivenQuery(queryIdx);
        }
    }

    @ParameterizedTest
    @CsvSource({"4"})
    public void generateCodeForGivenQuery(int queryIdx) throws Exception {
        try {
            generateCodeUsingMainFunction(queryIdx);
            copyGeneratedCodeToQueryFolder(queryIdx);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            removeEveryGeneratedCodeFolder();
        }
    }

    private static File getQueryFolderFile(int queryIdx) throws FileNotFoundException {
        File queryFolderFile = new File(RESOURCE_FOLDER + separator + QUERY_FOLDER_PREFIX + queryIdx);
        if (queryFolderFile.exists() && queryFolderFile.isDirectory()) {
            return queryFolderFile;
        } else {
            throw new FileNotFoundException("q" + queryIdx + " query folder does not exists.");
        }
    }

    private static File getQueryJsonFile(int queryIdx) throws FileNotFoundException {
        File queryFolderFile = getQueryFolderFile(queryIdx);
        File queryJsonFile = new File(queryFolderFile.getAbsolutePath()
                + separator
                + QUERY_JSON_FILE_PREFIX
                + queryIdx
                + JSON_FILE_SUFFIX);

        if (queryJsonFile.exists()) {
            return queryJsonFile;
        } else {
            throw new FileNotFoundException("q" + queryIdx + " query json file does not exists.");
        }
    }

    private static boolean validateQueryFolderExist(File file) {
        return file.isDirectory() && file.getName().substring(0, QUERY_FOLDER_PREFIX_LEN).equals(QUERY_FOLDER_PREFIX);
    }

    private static boolean validateQueryJsonFileExist(int queryIdx) throws Exception {
        return getQueryJsonFile(queryIdx).exists();
    }

    private void generateCodeUsingMainFunction(int queryIdx) throws Exception {
        File queryFolderFile = getQueryFolderFile(queryIdx);
        File queryJsonFile = getQueryJsonFile(queryIdx);

        String[] args = {
                queryJsonFile.getAbsolutePath(),
                queryFolderFile.getAbsolutePath(),
                FLINK_INPUT_FILE_PATH,
                FLINK_OUTPUT_FILE_PATH,
                "file"
        };
        CodeGen.main(args);
    }

    private void generateCodeUsingJar(int queryIdx) throws Exception {
        File queryFolderFile = getQueryFolderFile(queryIdx);
        File queryJsonFile = getQueryJsonFile(queryIdx);

        // repackage the codegen jar
        runCommand("mvn package -DskipTests -f .");

        // run the codegen.jar
        String commandStr = getCodegenCommandStr(CODEGEN_JAR_PATH,
                queryJsonFile.getAbsolutePath(),
                queryFolderFile.getAbsolutePath(),
                FLINK_INPUT_FILE_PATH,
                FLINK_OUTPUT_FILE_PATH,
                "file");
        runCommand(commandStr);
    }

    private String getCodegenCommandStr(String codegenJarPath,
                                        String queryJsonFilePath,
                                        String queryFolderPath,
                                        String flinkInputFilePath,
                                        String flinkOutputFilePath,
                                        String mode) {
        StringBuffer sb = new StringBuffer("java -jar ");
        sb.append(codegenJarPath);
        sb.append(" ");
        sb.append(queryJsonFilePath);
        sb.append(" ");
        sb.append(queryFolderPath);
        sb.append(" ");
        sb.append(flinkInputFilePath);
        sb.append(" ");
        sb.append(flinkOutputFilePath);
        sb.append(" ");
        sb.append(mode);
        return sb.toString();
    }


    private void runCommand(String command_str) throws IOException, InterruptedException {
        Process process = Runtime.getRuntime().exec(command_str);

        System.out.println("run command: " + command_str);

        try {
            BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));
            BufferedReader stdError = new BufferedReader(new InputStreamReader(process.getErrorStream()));

            String processOutput;
            while ((processOutput = stdInput.readLine()) != null) {
                System.out.println(processOutput);
            }
            while ((processOutput = stdError.readLine()) != null) {
                System.out.println("ERROR:" + processOutput);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                process.getInputStream().close();
                process.getErrorStream().close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        int exitCode = process.waitFor();

        if (exitCode == 0) {
            System.out.println("The command was executed successfully.");
        } else {
            System.err.println("The command failed, exit code = " + exitCode + ".");
        }

    }


    private void copyGeneratedCodeToQueryFolder(int queryIdx) throws Exception {
        File queryFolderFile = getQueryFolderFile(queryIdx);
        File generatedCodeSourceFolder = new File(queryFolderFile.getAbsolutePath() + separator
                + GENERATED_CODE + separator + "src" + separator + "main" + separator
                + "scala" + separator + "org" + separator + "hkust" + separator);
        File[] files = generatedCodeSourceFolder.listFiles();
        for (File file : files) {
            File dstFile = new File(queryFolderFile.getAbsolutePath() + separator + file.getName());
            Files.copy(file.toPath(), dstFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static void removeGeneratedCodeFolder(int queryIdx) throws Exception {
        File queryFolderFile = getQueryFolderFile(queryIdx);
        File generatedCodeFolder = new File(queryFolderFile.getAbsolutePath() + separator + GENERATED_CODE);
        if (generatedCodeFolder.isDirectory() && generatedCodeFolder.exists()) {
            FileUtils.deleteDirectory(generatedCodeFolder);
        }
    }

    private static void removeEveryGeneratedCodeFolder() throws Exception {
        for (int queryIdx : queryArrayList) {
            removeGeneratedCodeFolder(queryIdx);
        }
    }
}
