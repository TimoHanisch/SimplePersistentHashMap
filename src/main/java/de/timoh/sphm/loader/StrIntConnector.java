package de.timoh.sphm.loader;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
    public MapConnector<String, Integer> load() throws SQLException {
        getMap().clear();
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
        return this;
    }

    @Override
    public MapConnector<String, Integer> forceSynchronization() throws SQLException {
        for (String s : getMap().keySet()) {
            String stm = "SELECT insertIfExistsStrInt('" + s + "', " + getMap().get(s) + ");";
            getConnectorInfo().getConnection().prepareStatement(stm).execute();
        }
        return this;
    }

    @Override
    public MapConnector<String, Integer> initialize(Map<String, Integer> map) throws Exception {
        setMap(map);
        getConnectorInfo().getConnection().prepareStatement(getInsertIfExsists(getConnectorInfo().getTableName())).execute();
        if (!getConnectorInfo().tableExists()) {
            getConnectorInfo().getConnection().prepareStatement(getCreateTable(getConnectorInfo().getTableName())).execute();
        }
        return this;
    }

    @Override
    public MapConnector<String, Integer> put(String key, Integer value) throws SQLException {
        String stm;
        if (getMap().containsKey(key)) {
            stm = "SELECT insertIfExistsStrInt('" + key + "', " + value + ");";
        } else {
            stm = "INSERT INTO " + getConnectorInfo().getTableName() + "(key,value) VALUES ('" + key + "'," + value + ");";
        }
        getConnectorInfo().getConnection().prepareStatement(stm).execute();
        return this;
    }

    @Override
    public MapConnector<String, Integer> remove(String key) throws SQLException {
        if (getMap().containsKey(key)) {
            String stm = "DELETE FROM "+getConnectorInfo().getTableName()+" WHERE key = '" + key + "'";
            getConnectorInfo().getConnection().prepareStatement(stm).execute();
        }
        return this;
    }

    private static String getCreateTable(String tableName) {
        return "CREATE TABLE " + tableName + "(\n"
                + "key varchar(255) PRIMARY KEY,\n"
                + "value integer\n"
                + ");";

    }

    private static String getInsertIfExsists(String tableName) {
        return "CREATE OR REPLACE FUNCTION insertIfExistsStrInt(k varchar(255),v integer) \n"
                + "        RETURNS integer AS $$\n"
                + "	BEGIN \n"
                + "		DELETE FROM " + tableName + " WHERE key = k;\n"
                + "		INSERT INTO " + tableName + "(key,value) VALUES (k,v);\n"
                + "		RETURN v;\n"
                + "	END;\n"
                + "	$$ LANGUAGE plpgsql;";
    }
}
