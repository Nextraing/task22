package tests;

import dbConnect.JDBCConnection;
import org.junit.jupiter.api.*;

import static utils.Log.LOG;

public class SetupWorldTests {

    @BeforeEach
    public void setUp(TestInfo testInfo) {

        LOG.info("_____ Start of '{}' test. _____", testInfo.getDisplayName());
        JDBCConnection.openDBConnection();
    }

    @AfterEach
    public void tearDown(TestInfo testInfo) {

        JDBCConnection.closeDBConnection();
        LOG.info("_____ Finish of '{}' test. _____", testInfo.getDisplayName());
    }
}
