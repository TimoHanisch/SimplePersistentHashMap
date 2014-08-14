package de.timoh.sphm.loader;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author Timo Hanisch (timohanisch@gmail.com)
 */
public class ConnectorInformation {

    private final Connection connection;

    private final String tableName;

    public ConnectorInformation(String dbUrl, String dbUser, String dbPw, String tableName) throws SQLException {
        this.connection = DriverManager.getConnection(dbUrl, dbUser, dbPw);
        this.tableName = tableName;
    }

    public boolean tableExists() throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        // Iterate through all existing tables to check if the one already exists.
        ResultSet tables = metaData.getTables(null, null, "%", null);
        while (tables.next()) {
            if (tables.getString(3).equals(this.tableName)) {
                return true;
            }
        }
        return false;
    }

    public Connection getConnection() {
        return connection;
    }

    public String getTableName() {
        return tableName;
    }

    public void close() throws SQLException {
        connection.close();
    }
}
