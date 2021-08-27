package tests;

import io.qameta.allure.*;
import org.junit.jupiter.api.*;

import java.sql.ResultSet;

import static dbConnect.JDBCConnection.*;
import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Tests for 'world' DB in MySQL")
public class WorldDBTests extends SetupWorldTests {

    private final String testTableName = "government";

    @Test
    @Owner("Viktoriya")
    @Order(1)
    @Feature("Create table")
    @Severity(SeverityLevel.CRITICAL)
    @DisplayName("Create '" + testTableName + "' table and count number of tables")
    @Description("This test verifies the creation of the '"
            + testTableName + "' table (ID, FormName) by number of tables in DB.")
    void createTableAndCountNumberOfTablesTest() {

        int numberOfTablesBeforeQuery = getNumberOfTablesInDB();

        String query = "CREATE TABLE " + testTableName + " ("
                + "ID INT(3) NOT NULL,"
                + "FormName VARCHAR(45) NOT NULL,"
                + "PRIMARY KEY (id))";

        createTable(query);

        int numberOfTablesAfterQuery = getNumberOfTablesInDB();
        int expectedNumber = 1;

        assertEquals(expectedNumber, numberOfTablesAfterQuery - numberOfTablesBeforeQuery,
                "Table creation error.");
    }

    @Test
    @Owner("Viktoriya")
    @Order(10)
    @Feature("Create table")
    @Severity(SeverityLevel.CRITICAL)
    @DisplayName("Create '" + testTableName + "' table and verify its existence")
    @Description("This test verifies the creation of the '" + testTableName + "' table (ID, FormName) " +
            "by the existence of a table named '" + testTableName + "' in DB.")
    void createTableAndVerifyItsExistenceTest() {

        String query = "CREATE TABLE " + testTableName + " ("
                + "ID INT(3) NOT NULL,"
                + "FormName VARCHAR(45) NOT NULL,"
                + "PRIMARY KEY (id))";

        createTable(query);

        assertTrue(isTableExists(testTableName),
                "Table creation error.");
    }

    @Test
    @Owner("Viktoriya")
    @Order(2)
    @Feature("Select request")
    @Severity(SeverityLevel.NORMAL)
    @DisplayName("Select int value request from 'city' table")
    @Description("This test verifies that the int value of select request matches the expected value.")
    public void selectRequestTest() {

        String query = "SELECT ID " +
                "FROM city " +
                "WHERE Population = 3276207 and CountryCode = 'AUS'";

        int expectedID = 130;
        int actualID = getIntValueFromQuery(query, "ID");

        assertEquals(expectedID, actualID, "Select request error.");
    }

    @Test
    @Owner("Viktoriya")
    @Order(2)
    @Flaky
    @Feature("Select request")
    @Severity(SeverityLevel.MINOR)
    @DisplayName("Select string value request from '" + testTableName + "' table")
    @Description("This test verifies that the string value of select request matches the expected value.")
    public void selectRequestFlakyTest() {

        String query = "SELECT FormName " +
                "FROM " + testTableName + " " +
                "WHERE ID = 13";

        String expectedCountryCode = "Federation";
        String actualCountryCode = getStringValueFromQuery(query, "FormName");

        assertEquals(expectedCountryCode, actualCountryCode, "Select request error.");
    }

    @Test
    @Owner("Viktoriya")
    @Order(2)
    @Feature("Insert request")
    @Severity(SeverityLevel.NORMAL)
    @DisplayName("Insert request into '" + testTableName + "' table")
    @Description("This test verifies that the inserted data matches the data from the table.")
    public void insertRequestTest() {

        String insertQuery = "INSERT INTO government (FormName, ID) " +
                "SELECT DISTINCT GovernmentForm,  DENSE_RANK() OVER (ORDER BY  GovernmentForm) as ID " +
                "FROM country";

        sendInsertQuery(insertQuery);

        String query1 = "SELECT DISTINCT GovernmentForm as FormName, " +
                "DENSE_RANK() OVER (ORDER BY  GovernmentForm) as ID " +
                "FROM country";
        String query2 = "SELECT * FROM " + testTableName;

        assertTrue(compareGovernmentRequestResults(query1, query2), "Insert request error.");
    }

    @Test
    @Owner("Viktoriya")
    @Order(3)
    @Feature("Update request")
    @Severity(SeverityLevel.NORMAL)
    @DisplayName("Update request to '" + testTableName + "' table")
    @Description("This test verifies that the data from update request matches the data in the table.")
    public void updateRequestTest() {

        String expectedFormMane = "People's Republic";
        String updateQuery = "UPDATE " + testTableName + " " +
                "SET FormName = \"" + expectedFormMane + "\" " +
                "WHERE ID=28";
        String selectQuery = "SELECT FormName " +
                "FROM " + testTableName + " " +
                "WHERE ID=28";

        sendUpdateQuery(updateQuery);

        String actualFormName = getStringValueFromQuery(selectQuery, "FormName");

        assertEquals(expectedFormMane, actualFormName, "Table updating error.");
    }

    @Test
    @Owner("Viktoriya")
    @Order(3)
    @Features({@Feature("Select request"), @Feature("Join")})
    @Severity(SeverityLevel.NORMAL)
    @DisplayName("Select join request from 'city','country','countrylanguage','" + testTableName + "' tables")
    @Description("This test verifies that the select join request matches the expected data.")
    public void selectJoinRequestTest() {

        String selectJoinQuery = "SELECT ci.ID, ci.Name as CityName, co.Name as CountryName, co.Continent, " +
                "g.ID as GovernmentFormID, g.FormName as GovernmentForm, ci.Population, " +
                "group_concat(cl.language SEPARATOR ', ') as Language " +
                "FROM city ci " +
                "LEFT JOIN country co ON co.Code=ci.CountryCode " +
                "LEFT JOIN countrylanguage cl ON cl.CountryCode=ci.CountryCode " +
                "LEFT JOIN " + testTableName + " g ON g.FormName=co.GovernmentForm " +
                "WHERE ci.ID = 34";

        ResultSet rs = sendSelectQuery(selectJoinQuery);

        assertAll("Should return inserted data",
                () -> assertEquals(34, rs.getInt("ID")),
                () -> assertEquals("Tirana", rs.getString("CityName")),
                () -> assertEquals("Albania", rs.getString("CountryName")),
                () -> assertEquals("Europe", rs.getString("Continent")),
                () -> assertEquals(29, rs.getInt("GovernmentFormID")),
                () -> assertEquals("Republic", rs.getString("GovernmentForm")),
                () -> assertEquals(270000, rs.getInt("Population")),
                () -> assertEquals("Albaniana, Greek, Macedonian", rs.getString("Language")));
    }

    @Test
    @Owner("Viktoriya")
    @Order(4)
    @Feature("Delete request")
    @Severity(SeverityLevel.NORMAL)
    @DisplayName("Delete request to '" + testTableName + "' table")
    @Description("This test verifies that the data from delete request does not present in the table.")
    public void testDeleteRequest() {

        String deleteQuery = "DELETE " +
                "FROM " + testTableName;
        String selectQuery = "SELECT * FROM " + testTableName;

        sendDeleteRequest(deleteQuery);

        int expectedNumberOfRows = 0;
        int actualNumberOfRows = countNumberOfRows(selectQuery);

        assertEquals(expectedNumberOfRows, actualNumberOfRows, "Delete request error.");
    }

    @Test
    @Owner("Viktoriya")
    @Order(9)
    @Feature("Drop table")
    @Severity(SeverityLevel.NORMAL)
    @DisplayName("Drop '" + testTableName + "' table and count number of tables")
    @Description("This test verifies that the '" + testTableName + "' table is dropped by number of tables in DB.")
    void dropTableAndCountNumberOfTablesTest() {

        int numberOfTablesBeforeQuery = getNumberOfTablesInDB();

        dropTable(testTableName);

        int numberOfTablesAfterQuery = getNumberOfTablesInDB();
        int expectedNumber = 1;

        assertEquals(expectedNumber, numberOfTablesBeforeQuery - numberOfTablesAfterQuery,
                "Table dropping error.");
    }

    @Test
    @Owner("Viktoriya")
    @Order(11)
    @Feature("Drop table")
    @Severity(SeverityLevel.NORMAL)
    @DisplayName("Drop '" + testTableName + "' table and verify its existence")
    @Description("This test verifies that the '" + testTableName + "' table is dropped " +
            "by the existence of a table named '" + testTableName + "' in DB.")
    void dropTableAndVerifyItsExistenceTest() {

        dropTable(testTableName);

        assertFalse(isTableExists(testTableName), "Table dropping error.");
    }
}
