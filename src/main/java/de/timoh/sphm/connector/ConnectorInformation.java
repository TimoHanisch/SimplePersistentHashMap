package de.timoh.sphm.connector;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.cpdsadapter.DriverAdapterCPDS;
import org.apache.commons.dbcp2.datasources.SharedPoolDataSource;

/**
 * This class stores some information about the database connectiong. 
 * @author <a href="mailto:timohanisch@gmail.com">Timo Hanisch</a>
 */
public class ConnectorInformation {

    private static final Map<String, DataSource> dataSourceMap = Collections.synchronizedMap(new HashMap<String, DataSource>());

    private final String tableName;

    private final DataSource dataSource;

    public ConnectorInformation(String dbUrl, String dbUser, String dbPw, String tableName) {
        if (dataSourceMap.containsKey(tableName)) {
            this.dataSource = dataSourceMap.get(tableName);
        } else {
            DriverAdapterCPDS cpds = new DriverAdapterCPDS();
            try {
                cpds.setDriver("org.postgresql.Driver");
            } catch (ClassNotFoundException ex) {
                throw new RuntimeException(ex);
            }
            cpds.setUrl(dbUrl);
            cpds.setUser(dbUser);
            cpds.setPassword(dbPw);

            SharedPoolDataSource tds = new SharedPoolDataSource();
            tds.setConnectionPoolDataSource(cpds);
            tds.setMaxTotal(10);
            tds.setMaxConnLifetimeMillis(1000);
            
            dataSource = tds;
            dataSourceMap.put(tableName, dataSource);
        }

        this.tableName = tableName;
    }

    /**
     * Checks whether the table connected with this information exists in the 
     * database.
     * 
     * @return
     * @throws SQLException 
     */
    public boolean tableExists() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            // Iterate through all existing tables to check if the one already exists.
            try (ResultSet tables = metaData.getTables(null, null, "%", null)) {
                while (tables.next()) {
                    String localName = tables.getString(3);
                    if (localName.equals(this.tableName.toLowerCase())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Returns the actual connection object.
     * 
     * @return 
     * @throws SQLException
     */
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    /**
     * Gets the table name connected with this connection information.
     * 
     * @return 
     */
    public String getTableName() {
        return tableName;
    }
}
