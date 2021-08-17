package dbConnect;

import com.mysql.cj.jdbc.MysqlDataSource;
import io.qameta.allure.Step;
import tables.Government;
import utils.PropertiesLoader;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static utils.Log.LOG;

public class JDBCConnection {

    private static final PropertiesLoader propertiesLoader = new PropertiesLoader();

    private static final String JDBC_PROTOCOL = propertiesLoader.getDBProperty("db.protocol");
    private static final String JDBC_DRIVER = propertiesLoader.getDBProperty("db.driver");
    private static final String HOST = propertiesLoader.getDBProperty("db.host");
    private static final String PORT = propertiesLoader.getDBProperty("db.port");
    private static final String DB_NAME = propertiesLoader.getDBProperty("db.name");

    private static final String URL = JDBC_PROTOCOL + ":" + JDBC_DRIVER + "://" + HOST + ":" + PORT + "/" + DB_NAME;
    private static final String USER = propertiesLoader.getDBProperty("db.username");
    private static final String PASSWORD = propertiesLoader.getDBProperty("db.userpassword");

    private static Connection con = null;
    private static Statement stmt = null;
    private static ResultSet rs = null;

    @Step("Open database connection.")
    public static void openDBConnection() {

        MysqlDataSource mysqlDataSource = new MysqlDataSource();
        mysqlDataSource.setUser(USER);
        mysqlDataSource.setPassword(PASSWORD);
        mysqlDataSource.setDatabaseName(DB_NAME);

        LOG.info("Connect to '{}' by user '{}'.", URL, USER);

        try {
            con = mysqlDataSource.getConnection();
            LOG.info("Connection to '{}' DB established successfully.", DB_NAME);

        } catch (SQLException sqlException) {

            LOG.error("Connection to '{}' BD fail:\n {}.", DB_NAME, sqlException.getMessage());
        }
    }

    @Step("Create table.")
    public static void createTable(String query) {

        try {
            stmt = con.prepareStatement(query);
            LOG.info("Send request to '{}' DB:\n {};", DB_NAME, query);
            stmt.executeUpdate(query);
            LOG.info("The table was created successfully.");

        } catch (SQLException sqlException) {

            LOG.error("Fail to create table:\n {}.", sqlException.getMessage());
        }
    }

    @Step("Drop table.")
    public static void dropTable(String tableName) {

        String query = "DROP TABLE " + tableName;

        try {
            stmt = con.prepareStatement(query);
            LOG.info("Send request to '{}' DB: {};", DB_NAME, query);
            stmt.executeUpdate(query);
            LOG.info("The '{}' table deleted successfully.", tableName);

        } catch (SQLException sqlException) {

            LOG.error("Fail to delete '{}' table:\n {}.", tableName, sqlException.getMessage());
        }
    }

    @Step("Send select query.")
    public static ResultSet sendSelectQuery(String query) {

        try {
            stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            LOG.info("Send request to '{}' DB:\n {};", DB_NAME, query);
            rs = stmt.executeQuery(query);
            LOG.info("The select request was processed successfully.");
            rs.next();

        } catch (SQLException sqlException) {

            LOG.error("Fail to select data from table:\n {}.", sqlException.getMessage());
        }

        return rs;
    }

    @Step("Send insert query.")
    public static void sendInsertQuery(String query) {

        try {
            stmt = con.createStatement();
            LOG.info("Send insert request to '{}' DB:\n {};", DB_NAME, query);
            stmt.executeUpdate(query);
            LOG.info("The insert request was processed successfully.");

        } catch (SQLException sqlException) {

            LOG.error("Fail to insert data into a table:\n {}.", sqlException.getMessage());
        }
    }

    @Step("Send update query.")
    public static void sendUpdateQuery(String query) {

        try {
            stmt = con.createStatement();
            LOG.info("Send update request to '{}' DB:\n {};", DB_NAME, query);
            stmt.executeUpdate(query);
            LOG.info("The update request was processed successfully.");

        } catch (SQLException sqlException) {

            LOG.error("Fail to update data in a table:\n {}.", sqlException.getMessage());
        }
    }

    @Step("Send delete query.")
    public static void sendDeleteRequest(String query) {

        try {
            stmt = con.createStatement();
            LOG.info("Send delete request to '{}' DB:\n {};", DB_NAME, query);
            stmt.executeUpdate(query);
            LOG.info("The delete request was processed successfully.");

        } catch (SQLException sqlException) {

            LOG.error("Fail to delete data in a table:\n {}.", sqlException.getMessage());
        }
    }

    @Step("Compare request results.")
    public static boolean compareGovernmentRequestResults(String query1, String query2) {

        return requestResultsToList(query1).equals(requestResultsToList(query2));
    }

    @Step("Transfer request data to ArrayList.")
    public static List<Government> requestResultsToList(String query) {

        List<Government> governmentList = new ArrayList<>();

        rs = sendSelectQuery(query);

        try {
            while (rs.next()) {
                governmentList.add(new Government(rs.getInt("ID"),
                        rs.getString("FormName")));
            }
            LOG.info("Transfer request data to ArrayList was processed successfully.");

        } catch (SQLException sqlException) {

            LOG.error("Fail to transfer data to ArrayList from request:\n {}.", sqlException.getMessage());
        }

        return governmentList;
    }

    public static Integer getIntValueFromQuery(String query, String attributeName) {

        int value = 0;

        try {
            value = sendSelectQuery(query).getInt(attributeName);
            LOG.info("Value of '{}' is {}.", attributeName, value);

        } catch (SQLException sqlException) {

            LOG.error("Fail to get value of '{}' attribute:\n {}.", attributeName, sqlException.getMessage());
        }

        return value;
    }

    public static String getStringValueFromQuery(String query, String attributeName) {

        String value = null;

        try {
            value = sendSelectQuery(query).getString(attributeName);
            LOG.info("Value of '{}' is '{}'.", attributeName, value);

        } catch (SQLException sqlException) {

            LOG.error("Fail to get value of '{}' attribute:\n {}.", attributeName, sqlException.getMessage());
        }

        return value;
    }

    @Step("Count number of tables in database.")
    public static Integer getNumberOfTablesInDB() {

        int numberOfTables = 0;
        String query = "SELECT count(*) as NumberOfTables " +
                "FROM information_schema.tables " +
                "WHERE table_schema='" + DB_NAME + "'";

        try {
            numberOfTables = sendSelectQuery(query).getInt("NumberOfTables");
            LOG.info("Current number of tables in '{}' DB is: {}.", DB_NAME, numberOfTables);

        } catch (SQLException sqlException) {

            LOG.error("Fail to count number of tables in '{}' DB:\n {}.", DB_NAME, sqlException.getMessage());
        }

        return numberOfTables;
    }

    @Step("Table existence check.")
    public static boolean isTableExists(String tableName) {

        boolean isExists = false;
        String query = "SELECT count(*) as NumberOfTables "
                + "FROM information_schema.tables "
                + "WHERE table_name = '" + tableName + "';";

        try {
            isExists = sendSelectQuery(query).getInt(1) != 0;
            LOG.info("Existence of the '{}' table: {}.", tableName, isExists);

        } catch (SQLException sqlException) {

            LOG.error("Fail to verify the existence of '{}' table:\n {}.", tableName, sqlException.getMessage());
        }

        return isExists;
    }

    @Step("Count number of rows in a table.")
    public static Integer countNumberOfRows(String query) {

        int numberOfRows = 0;

        rs = sendSelectQuery(query);

        try {
            while (rs.next()) {
                numberOfRows++;
            }
            LOG.info("Number of rows is: {}.", numberOfRows);

        } catch (SQLException sqlException) {

            LOG.error("Fail to count number of rows in a table:\n {}.", sqlException.getMessage());
        }

        return numberOfRows;
    }

    public static void closeDBConnection() {

        closeResultSet();
        closeStatement();
        closeConnection();

    }

    @Step("Close Connection.")
    private static void closeConnection() {

        if (con != null) {
            try {
                con.close();
                LOG.info("Connection to '{}' DB closed successfully.", DB_NAME);

            } catch (SQLException sqlException) {

                LOG.error("Fail to close connection to '{}' DB:\n {}.", DB_NAME, sqlException.getMessage());
            }
        }
    }

    @Step("Close Statement.")
    private static void closeStatement() {

        if (stmt != null) {
            try {
                stmt.close();
                LOG.info("Statement closed successfully.");

            } catch (SQLException sqlException) {

                LOG.error("Fail to close statement:\n {}.", sqlException.getMessage());
            }
        }
    }

    @Step("Close ResultSet.")
    private static void closeResultSet() {

        if (rs != null) {
            try {
                rs.close();
                LOG.info("ResultSet closed successfully.");

            } catch (SQLException sqlException) {

                LOG.error("Fail to close ResultSet:\n {}.", sqlException.getMessage());
            }
        }
    }
}
