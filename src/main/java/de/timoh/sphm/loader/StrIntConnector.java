package de.timoh.sphm.loader;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Timo Hanisch (timohanisch@gmail.com)
 */
public class StrIntConnector extends MapConnector<String, Integer> {

    public StrIntConnector(ConnectorInformation connectorInfo) {
        super(connectorInfo);
    }

    @Override
    public void load() throws SQLException {
        String stm = "SELECT * FROM " + getConnectorInfo().getTableName();
        String key;
        Integer value;
        PreparedStatement pStm = getConnectorInfo().getConnection().prepareStatement(stm);
        ResultSet resultSet = pStm.executeQuery();
        while (resultSet.next()) {
            key = resultSet.getString("key");
            value = resultSet.getInt("value");
            getMap().put(key, value);
        }
    }

    @Override
    public void forceSynchronization() throws SQLException {
        for (String s : getMap().keySet()) {
            String stm = "SELECT insertIfExists('" + s + "', " + getMap().get(s) + ");";
            if (!getConnectorInfo().getConnection().prepareStatement(stm).execute()) {
                throw new SQLException("Could not execute statement");
            }
        }
    }

    @Override
    public void initialize(Map<String, Integer> map) throws Exception {
        setMap(map);
        getConnectorInfo().getConnection().prepareStatement(getInsertIfExsists(getConnectorInfo().getTableName())).execute();
        if (!getConnectorInfo().tableExists()) {
            getConnectorInfo().getConnection().prepareStatement(getCreateTable(getConnectorInfo().getTableName())).execute();
        }
    }

    @Override
    public void put(String key, Integer value) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Integer remove(String key) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    private static String getCreateTable(String tableName) {
        return "CREATE TABLE "+tableName+"(\n"
                + "key varchar(255) PRIMARY KEY,\n"
                + "value integer\n"
                + ");";
                
    }
    
    private static String getInsertIfExsists(String tableName) {
        return "CREATE OR REPLACE FUNCTION insertIfExistsStrInt(k varchar(255),v integer) \n"
                + "        RETURNS integer AS $$\n"
                + "	BEGIN \n"
                + "		DELETE FROM "+tableName+" WHERE key = k;\n"
                + "		INSERT INTO "+tableName+"(key,count) VALUES (k,v);\n"
                + "		RETURN v;\n"
                + "	END;\n"
                + "	$$ LANGUAGE plpgsql;";
    }
}
