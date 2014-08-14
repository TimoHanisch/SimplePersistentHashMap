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
    public void load() throws SQLException {
        String stm = "SELECT * FROM persistent_map";
        String key;
        Integer value;
        PreparedStatement pStm = getConnectorInfo().getConnection().prepareStatement(stm);
        ResultSet resultSet = pStm.executeQuery();
        while (resultSet.next()) {
            key = resultSet.getString("key");
            value = resultSet.getInt("count");
            getMap().put(key, value);
        }
    }

    @Override
    public void forceSynchronization() throws SQLException {
        try {
            for (String s : getMap().keySet()) {
                String stm = "SELECT insertIfExists('" + s + "', " + getMap().get(s) + ");";
                if (!getConnectorInfo().getConnection().prepareStatement(stm).execute()) {
                    throw new SQLException("Could not execute statement");
                }
            }
        } catch (SQLException e) {
            throw e;
        }
    }

    @Override
    public void initialize(Map<String, Integer> map) throws SQLException {
        setMap(map);
    }

    @Override
    public void put(String key, Integer value) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Integer remove(String key) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
