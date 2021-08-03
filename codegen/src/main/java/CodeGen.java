import com.google.common.collect.ImmutableSet;
import org.hkust.checkerutils.CheckerUtils;
import org.hkust.codegenerator.CodeGenerator;
import org.hkust.parser.Parser;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Set;


@Command(name = "CodeGen", version = "CodeGenCli 0.1", mixinStandardHelpOptions = true)
class CodeGen implements Runnable {
    private static final Set<String> IO_TYPES = ImmutableSet.of("file", "socket", "kafka");

    @Option(names = {"-q", "--SQL"}, required = false, description = "Given SQL query, optional")
    String sql;

    @Option(names = {"-j", "--json-file"}, required = true, description = "Input json file path, if SQL query is given, " +
            "then generate the json in the given path")
    String jsonFile = "./input_json_file.json";

    @Option(names = {"-g", "--generated-jar"}, required = true, description = "Generated code directory path")
    String generatedDirectoryPath = ".";

    @Option(names = {"-i", "--flink-input"}, required = true, description = "Flink data input file path")
    String flinkInputPath = "file:///input.csv";

    @Option(names = {"-o", "--flink-output"}, required = true, description = "Flink data output file path")
    String flinkOutputPath = "file:///output.csv";

    @Option(names = {"-s", "--data-sink"}, arity = "1..3", required = true, description = "Flink data sink types, " +
            "including file, socket, kafka")
    String[] dataSinkTypes = {"file", "socket"};

    public static void main(String[] args) throws Exception {
        int exitCode = new CommandLine(new CodeGen()).execute(args);
        System.exit(exitCode);
    }

    private static void prepareEnvironment(String jarPath) throws IOException, URISyntaxException {
        StringBuilder generatedCodeBuilder = new StringBuilder();
        String generatedCode = "generated-code";
        generatedCodeBuilder.append(jarPath)
                .append(File.separator)
                .append(generatedCode)
                .append(File.separator)
                .append("src")
                .append(File.separator)
                .append("main")
                .append(File.separator)
                .append("scala")
                .append(File.separator)
                .append("org")
                .append(File.separator)
                .append("hkust");

        File directory = new File(generatedCodeBuilder.toString());
        if (!directory.exists()) {
            directory.mkdirs();
        }
        extractPomFile(jarPath + File.separator + generatedCode);
    }

    static void extractPomFile(String path) throws IOException {
        String pomName = "pom.xml";
        String content = "<project xmlns=\"http://maven.apache.org/POM/4.0.0\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
                "         xsi:schemaLocation=\"http://maven.apache.org/POM/4.0.0 http://maven.apache.org/maven-v4_0_0.xsd\">\n" +
                "    <modelVersion>4.0.0</modelVersion>\n" +
                "    <groupId>org.hkust</groupId>\n" +
                "    <artifactId>generated-code</artifactId>\n" +
                "    <version>1.0-SNAPSHOT</version>\n" +
                "    <name>${project.artifactId}</name>\n" +
                "\n" +
                "    <properties>\n" +
                "        <maven.compiler.source>1.8</maven.compiler.source>\n" +
                "        <maven.compiler.target>1.8</maven.compiler.target>\n" +
                "        <encoding>UTF-8</encoding>\n" +
                "        <scala.version>2.12.6</scala.version>\n" +
                "        <scala.compat.version>2.12</scala.compat.version>\n" +
                "        <spec2.version>4.2.0</spec2.version>\n" +
                "    </properties>\n" +
                "\n" +
                "    <dependencies>\n" +
                "        <dependency>\n" +
                "            <groupId>org.hkust</groupId>\n" +
                "            <artifactId>AJU</artifactId>\n" +
                "            <version>1.0-SNAPSHOT</version>\n" +
                "        </dependency>\n" +
                "        <dependency>\n" +
                "            <groupId>org.apache.flink</groupId>\n" +
                "            <artifactId>flink-streaming-scala_2.12</artifactId>\n" +
                "            <version>1.11.2</version>\n" +
                "            <scope>provided</scope>\n" +
                "        </dependency>\n" +
                "    </dependencies>\n" +
                "\n" +
                "    <build>\n" +
                "        <sourceDirectory>src/main/scala</sourceDirectory>\n" +
                "        <plugins>\n" +
                "            <plugin>\n" +
                "                <!-- see http://davidb.github.com/scala-maven-plugin -->\n" +
                "                <groupId>net.alchim31.maven</groupId>\n" +
                "                <artifactId>scala-maven-plugin</artifactId>\n" +
                "                <version>3.3.2</version>\n" +
                "                <executions>\n" +
                "                    <execution>\n" +
                "                        <goals>\n" +
                "                            <goal>compile</goal>\n" +
                "                        </goals>\n" +
                "                    </execution>\n" +
                "                </executions>\n" +
                "            </plugin>\n" +
                "            <plugin>\n" +
                "                <groupId>org.apache.maven.plugins</groupId>\n" +
                "                <artifactId>maven-surefire-plugin</artifactId>\n" +
                "                <version>2.21.0</version>\n" +
                "                <configuration>\n" +
                "                    <!-- Tests will be run with scalatest-maven-plugin instead -->\n" +
                "                    <skipTests>true</skipTests>\n" +
                "                </configuration>\n" +
                "            </plugin>\n" +
                "            <plugin>\n" +
                "                <groupId>org.apache.maven.plugins</groupId>\n" +
                "                <artifactId>maven-assembly-plugin</artifactId>\n" +
                "                <version>2.4</version>\n" +
                "                <configuration>\n" +
                "                    <descriptorRefs>\n" +
                "                        <descriptorRef>jar-with-dependencies</descriptorRef>\n" +
                "                    </descriptorRefs>\n" +
                "                    <archive>\n" +
                "                        <manifest>\n" +
                "                            <mainClass>Job</mainClass>\n" +
                "                        </manifest>\n" +
                "                    </archive>\n" +
                "                </configuration>\n" +
                "                <executions>\n" +
                "                    <execution>\n" +
                "                        <phase>package</phase>\n" +
                "                        <goals>\n" +
                "                            <goal>single</goal>\n" +
                "                        </goals>\n" +
                "                    </execution>\n" +
                "                </executions>\n" +
                "            </plugin>\n" +
                "        </plugins>\n" +
                "    </build>\n" +
                "</project>\n";
        File target = new File(path + File.separator + pomName);

        FileOutputStream outputStream = new FileOutputStream(target);
        byte[] bytes = content.getBytes();
        outputStream.write(bytes);
        outputStream.close();
    }

    private static void validateOptions(String jsonFile, String generatedDirectoryPath, String flinkInputPath, String flinkOutputPath, String[] dataSinkTypes) {
        validateJsonFile(jsonFile);
        validateDirectoryPath(generatedDirectoryPath);
        CheckerUtils.checkNullOrEmpty(flinkInputPath, "flinkInputPath");
        CheckerUtils.checkNullOrEmpty(flinkOutputPath, "flinkOutputPath");
        validateFlinkIOType(dataSinkTypes);
    }

    private static void validateJsonFile(String jsonFilePath) {
        CheckerUtils.checkNullOrEmpty(jsonFilePath, "jsonFilePath");
        if (jsonFilePath.endsWith(".json")) {
            if (new File(jsonFilePath).exists()) return;
            throw new RuntimeException("Unable to find JSON file");
        } else {
            throw new RuntimeException("Path provided isn't for a .json file: " + jsonFilePath);
        }
    }

    private static void validateDirectoryPath(String directoryPath) {
        CheckerUtils.checkNullOrEmpty(directoryPath, "directoryPath");
        File outputDir = new File(directoryPath);
        if (outputDir.exists() && outputDir.isDirectory()) return;

        throw new RuntimeException("output directory must exist and must be a directory, got: " + directoryPath);
    }

    private static void validateFlinkIOType(String ioType) {
        CheckerUtils.checkNullOrEmpty(ioType, "ioType");
        if (!IO_TYPES.contains(ioType.toLowerCase())) {
            throw new RuntimeException("Only the following flink IO types are supported: " + Arrays.toString(IO_TYPES.toArray()));
        }
    }

    private static void validateFlinkIOType(String[] dataSinkTypes) {
        for (String sinkType : dataSinkTypes) {
            validateFlinkIOType(sinkType);
        }
    }

    @Override
    public void run() {
        System.out.println("\nCquirrel -- CodeGen\n");
        if (sql != null) {
            System.out.println(sql);
            try {
                Parser parser = new Parser(sql, jsonFile);
                parser.parse();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        validateOptions(jsonFile, generatedDirectoryPath, flinkInputPath, flinkOutputPath, dataSinkTypes);
        try {
            prepareEnvironment(generatedDirectoryPath);
            CodeGenerator.generate(jsonFile, generatedDirectoryPath, flinkInputPath, flinkOutputPath, dataSinkTypes);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

