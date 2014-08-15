package de.timoh.sphm.loader;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.cpdsadapter.DriverAdapterCPDS;
import org.apache.commons.dbcp2.datasources.SharedPoolDataSource;

/**
 *
 * @author Timo Hanisch (timohanisch@gmail.com)
 */
public class ConnectorInformation {

    private static final Map<String, DataSource> dataSourceMap = Collections.synchronizedMap(new HashMap<String, DataSource>());

    private final String tableName;

    private final DataSource dataSource;

    public ConnectorInformation(String dbUrl, String dbUser, String dbPw, String tableName) throws Exception {
        if (dataSourceMap.containsKey(tableName)) {
            this.dataSource = dataSourceMap.get(tableName);
        } else {
            DriverAdapterCPDS cpds = new DriverAdapterCPDS();
            cpds.setDriver("org.postgresql.Driver");
            cpds.setUrl(dbUrl);
            cpds.setUser(dbUser);
            cpds.setPassword(dbPw);

            SharedPoolDataSource tds = new SharedPoolDataSource();
            tds.setConnectionPoolDataSource(cpds);
            tds.setMaxTotal(10);

            dataSource = tds;
            dataSourceMap.put(tableName, dataSource);
        }

        this.tableName = tableName;
    }

    public boolean tableExists() throws Exception {
        try (Connection connection = dataSource.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            // Iterate through all existing tables to check if the one already exists.
            try (ResultSet tables = metaData.getTables(null, null, "%", null)) {
                while (tables.next()) {
                    String localName = tables.getString(3);
                    if (localName.equals(this.tableName)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public Connection getConnection() throws Exception {
        return dataSource.getConnection();
    }

    public String getTableName() {
        return tableName;
    }
}
