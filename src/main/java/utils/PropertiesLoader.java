package utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import static utils.Log.LOG;

public class PropertiesLoader {

    private final Properties properties = new Properties();

    public String getDBProperty(String propertyName) {

        try {
            properties.load(new FileInputStream("./src/main/resources/database.properties"));

        } catch (IOException ioException) {

            LOG.error("Problem with property file: ", ioException);
        }

        return properties.getProperty(propertyName);
    }

}
