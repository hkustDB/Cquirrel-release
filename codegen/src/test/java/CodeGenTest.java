import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CodeGenTest {
    private final String JSON_FILE_NAME = "q14/Q14.json";
    private String JSON_FILE_PATH;
    private String TEST_DIRECTORY;

    @Before
    public void setup() {
        final String resourceFolder = new File("src" + File.separator + "test" + File.separator + "resources").getAbsolutePath();
        JSON_FILE_PATH = resourceFolder + File.separator + JSON_FILE_NAME;
        TEST_DIRECTORY = resourceFolder;
    }
//
//    @Test
//    public void invalidJsonFile() {
//        String[] args = getArgs();
//        args[0] = args[0].substring(0, args[0].length() - 2);
//        exceptionTest(args, "Path provided isn't for a .json file");
//    }
//
//    @Test
//    public void noJsonFile() {
//        String[] args = getArgs();
//        args[0] = args[0].replace(JSON_FILE_NAME, "file.json");
//        exceptionTest(args, "Unable to find JSON file");
//    }
//
//    @Test
//    public void nullOrEmptyArgs() {
//        String[] args = getArgs();
//        args[0] = "";
//        exceptionTest(args, "cannot be null or empty");
//    }
//
//    @Test
//    public void extraArgs() {
//        String[] args = new String[6];
//        exceptionTest(args, "Expecting exactly 5 input strings");
//    }
//
//    @Test
//    public void invalidOutputDir() {
//        String[] args = getArgs();
//        args[1] = "invalidDirectory_";
//        exceptionTest(args, "output directory must exist and must be a directory");
//    }

    private void exceptionTest(String[] args, String exceptionMessage) {
        try {
            CodeGen.main(args);
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(exceptionMessage));
        }
    }

    @Test
    public void runCodeGenTest() throws Exception {
        CodeGen.main(getArgs());
    }

    @After
    public void cleanTestGarbage() throws IOException {
        FileUtils.deleteDirectory(
                new File("src" + File.separator + "test" + File.separator + "resources" + File.separator + "generated-code")
        );
    }

    private String[] getArgs() {
//        String[] args = new String[5];
//        args[0] = JSON_FILE_PATH;
//        args[1] = TEST_DIRECTORY;
//        args[2] = TEST_DIRECTORY;
//        args[3] = TEST_DIRECTORY;
//        args[4] = "File";
        String[] args = new String[]{"-j", JSON_FILE_PATH, "-g", TEST_DIRECTORY, "-i", TEST_DIRECTORY, "-o", TEST_DIRECTORY, "-s", "file"};
        return args;
    }
}
